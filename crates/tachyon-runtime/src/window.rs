//! Operador de ventanas en memoria.
//!
//! El estado dueño es el mapa por clave (`OperatorState`): eso es lo que se
//! clona al sidecar. `by_end` es un árbol B de fines de ventana hacia esas
//! mismas claves. No se persiste. Al restaurar se reconstruye caminando el mapa.
//!
//! Cerrar es un `split_off`: salen las ventanas con fin `<= watermark` y el
//! resto no se recorre. Tumbling y hop anotan el fin una sola vez. Session lo
//! mueve cuando el intervalo se estira o se fusiona.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use tachyon_core::{
    Accumulators, AggKind, AggSpec, AggState, KeyState, OperatorState, SessionState, WindowKind,
    WindowSpecId,
};

/// Una fila ya proyectada. `key` o `event_time_ms` en `None` es un drop
/// determinista: no mueve el reloj ni abre ventana.
#[derive(Debug, Clone)]
pub struct WindowInput {
    pub key: Option<Vec<u8>>,
    pub event_time_ms: Option<i64>,
    pub partition: i32,
    pub offset: i64,
    /// Paralelo a `spec.aggs`. `COUNT(*)` ignora el valor.
    pub values: Vec<Option<Num>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Num {
    I64(i64),
    F64(f64),
}

/// Ventana que acaba de cerrarse. El llamador arma el `RecordBatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClosedWindow {
    pub key: Vec<u8>,
    pub window_start: i64,
    pub window_end: i64,
    pub acc: Accumulators,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFault {
    Overflow {
        key: Vec<u8>,
        sum: i128,
        partition: i32,
        offset: i64,
    },
    TypeMix {
        partition: i32,
        offset: i64,
    },
    NonFinite {
        partition: i32,
        offset: i64,
    },
    ValueCount {
        expected: usize,
        got: usize,
    },
}

impl std::fmt::Display for WindowFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowFault::Overflow {
                sum,
                partition,
                offset,
                ..
            } => write!(
                f,
                "SUM no entra en i64 ({sum}) en la partición {partition} offset {offset}"
            ),
            WindowFault::TypeMix { partition, offset } => {
                write!(f, "mezcla Int64 y Float64 en partición {partition} offset {offset}")
            }
            WindowFault::NonFinite { partition, offset } => {
                write!(f, "Float64 no finito en partición {partition} offset {offset}")
            }
            WindowFault::ValueCount { expected, got } => {
                write!(f, "la fila trae {got} medidas y el spec pide {expected}")
            }
        }
    }
}

impl std::error::Error for WindowFault {}

#[derive(Debug, Clone)]
struct PartitionClock {
    max_event_time_ms: Option<i64>,
    last_on_time: Option<Instant>,
}

/// Estado vivo de una query de ventana.
#[derive(Debug, Clone)]
pub struct WindowOperator {
    spec: WindowSpecId,
    lag_ms: i64,
    idle: std::time::Duration,
    keys: BTreeMap<Vec<u8>, KeyState>,
    /// Fin de disparo (el `end` de tumble/hop, `end + gap` de session) → claves.
    by_end: BTreeMap<i64, BTreeSet<Vec<u8>>>,
    partitions: BTreeMap<i32, PartitionClock>,
    instance_watermark_ms: Option<i64>,
}

impl WindowOperator {
    pub fn new(spec: WindowSpecId, lag_ms: i64, idle_ms: i64) -> Self {
        Self {
            spec,
            lag_ms,
            idle: std::time::Duration::from_millis(idle_ms.max(0) as u64),
            keys: BTreeMap::new(),
            by_end: BTreeMap::new(),
            partitions: BTreeMap::new(),
            instance_watermark_ms: None,
        }
    }

    /// Restaura el mapa del sidecar y rearma `by_end`.
    pub fn restore(
        spec: WindowSpecId,
        lag_ms: i64,
        idle_ms: i64,
        state: OperatorState,
        instance_watermark_ms: Option<i64>,
    ) -> Self {
        let mut op = Self::new(spec, lag_ms, idle_ms);
        op.keys = state.keys;
        op.instance_watermark_ms = instance_watermark_ms;
        op.reindex();
        op
    }

    pub fn instance_watermark_ms(&self) -> Option<i64> {
        self.instance_watermark_ms
    }

    pub fn open_windows(&self) -> usize {
        self.keys
            .values()
            .map(|state| state.windows.len() + state.sessions.len())
            .sum()
    }

    /// Aplica el batch en orden. Si una fila falla, el operador vuelve al
    /// estado de antes del batch y no devuelve las ventanas que ese batch
    /// hubiera cerrado.
    pub fn apply(
        &mut self,
        rows: &[WindowInput],
        now: Instant,
    ) -> Result<Vec<ClosedWindow>, WindowFault> {
        let snapshot = self.clone();
        match self.apply_inner(rows, now) {
            Ok(closed) => Ok(closed),
            Err(err) => {
                *self = snapshot;
                Err(err)
            }
        }
    }

    /// Recalcula particiones idle. Puede cerrar ventanas si el mínimo sube.
    /// No cierra por el paso del tiempo de pared cuando todas están idle.
    pub fn on_tick(&mut self, now: Instant) -> Vec<ClosedWindow> {
        self.raise_and_close(now)
    }

    /// El mapa por clave, listo para el sidecar. `by_end` no entra.
    pub fn freeze_state(&self) -> OperatorState {
        OperatorState {
            keys: self.keys.clone(),
        }
    }

    fn apply_inner(
        &mut self,
        rows: &[WindowInput],
        now: Instant,
    ) -> Result<Vec<ClosedWindow>, WindowFault> {
        let mut closed = Vec::new();
        for row in rows {
            closed.extend(self.apply_row(row, now)?);
        }
        Ok(closed)
    }

    fn apply_row(
        &mut self,
        row: &WindowInput,
        now: Instant,
    ) -> Result<Vec<ClosedWindow>, WindowFault> {
        let (Some(key), Some(t)) = (&row.key, row.event_time_ms) else {
            return Ok(Vec::new());
        };
        if self
            .instance_watermark_ms
            .is_some_and(|watermark| t < watermark)
        {
            return Ok(Vec::new());
        }
        let clock = self.partitions.entry(row.partition).or_insert(PartitionClock {
            max_event_time_ms: None,
            last_on_time: None,
        });
        clock.max_event_time_ms = Some(clock.max_event_time_ms.map_or(t, |max| max.max(t)));
        clock.last_on_time = Some(now);
        self.assign(key, t, row)?;
        Ok(self.raise_and_close(now))
    }

    fn assign(&mut self, key: &Vec<u8>, t: i64, row: &WindowInput) -> Result<(), WindowFault> {
        match self.spec.kind {
            WindowKind::Tumble => {
                let start = align_down(t, self.spec.size_ms);
                self.touch_fixed(key, start, start + self.spec.size_ms, row)
            }
            WindowKind::Hop => {
                let slide = self.spec.slide_ms.expect("hop con slide");
                let size = self.spec.size_ms;
                let mut start = align_down(t, slide);
                let mut guard = 0;
                while start > t - size {
                    self.touch_fixed(key, start, start + size, row)?;
                    start -= slide;
                    guard += 1;
                    if guard > 10_000 {
                        break;
                    }
                }
                Ok(())
            }
            WindowKind::Session => {
                let gap = self.spec.gap_ms.expect("session con gap");
                self.touch_session(key, t, gap, row)
            }
        }
    }

    fn touch_fixed(
        &mut self,
        key: &Vec<u8>,
        start: i64,
        end: i64,
        row: &WindowInput,
    ) -> Result<(), WindowFault> {
        let state = self.keys.entry(key.clone()).or_insert_with(empty_key);
        let is_new = !state.windows.contains_key(&start);
        let acc = state
            .windows
            .entry(start)
            .or_insert_with(|| fresh_acc(&self.spec.aggs));
        absorb(acc, &self.spec.aggs, &row.values, row)?;
        if is_new {
            self.by_end.entry(end).or_default().insert(key.clone());
        }
        Ok(())
    }

    fn touch_session(
        &mut self,
        key: &Vec<u8>,
        t: i64,
        gap: i64,
        row: &WindowInput,
    ) -> Result<(), WindowFault> {
        let aggs = self.spec.aggs.clone();
        let state = self.keys.entry(key.clone()).or_insert_with(empty_key);
        let hit: Vec<usize> = state
            .sessions
            .iter()
            .enumerate()
            .filter(|(_, session)| intersects(session, t, gap))
            .map(|(index, _)| index)
            .collect();
        if hit.is_empty() {
            let mut acc = fresh_acc(&aggs);
            absorb(&mut acc, &aggs, &row.values, row)?;
            insert_sorted(
                &mut state.sessions,
                SessionState {
                    start_ms: t,
                    end_ms: t,
                    acc,
                },
            );
            self.by_end
                .entry(t.saturating_add(gap))
                .or_default()
                .insert(key.clone());
            return Ok(());
        }
        let mut removed = Vec::with_capacity(hit.len());
        for index in hit.into_iter().rev() {
            removed.push(state.sessions.remove(index));
        }
        removed.reverse();
        let mut merged = removed.remove(0);
        let mut old_fires = vec![merged.end_ms.saturating_add(gap)];
        for other in removed {
            old_fires.push(other.end_ms.saturating_add(gap));
            merge_acc(&mut merged.acc, &other.acc, row)?;
            merged.start_ms = merged.start_ms.min(other.start_ms);
            merged.end_ms = merged.end_ms.max(other.end_ms);
        }
        let mut contribution = fresh_acc(&aggs);
        absorb(&mut contribution, &aggs, &row.values, row)?;
        merge_acc(&mut merged.acc, &contribution, row)?;
        merged.start_ms = merged.start_ms.min(t);
        merged.end_ms = merged.end_ms.max(t);
        let new_fire = merged.end_ms.saturating_add(gap);
        insert_sorted(&mut state.sessions, merged);
        for fire in old_fires {
            unindex(&mut self.by_end, fire, key);
        }
        self.by_end.entry(new_fire).or_default().insert(key.clone());
        Ok(())
    }

    fn raise_and_close(&mut self, now: Instant) -> Vec<ClosedWindow> {
        let min = self
            .partitions
            .values()
            .filter(|clock| clock.is_active(now, self.idle))
            .filter_map(|clock| clock.max_event_time_ms)
            .map(|max| max.saturating_sub(self.lag_ms))
            .min();
        if let Some(min) = min {
            self.instance_watermark_ms = Some(match self.instance_watermark_ms {
                Some(current) => current.max(min),
                None => min,
            });
        }
        let Some(watermark) = self.instance_watermark_ms else {
            return Vec::new();
        };
        self.close_until(watermark)
    }

    fn close_until(&mut self, watermark: i64) -> Vec<ClosedWindow> {
        let later = if watermark == i64::MAX {
            BTreeMap::new()
        } else {
            self.by_end.split_off(&(watermark + 1))
        };
        let due = std::mem::replace(&mut self.by_end, later);
        let mut closed = Vec::new();
        for (fire_at, keys) in due {
            for key in keys {
                let Some(state) = self.keys.get_mut(&key) else {
                    continue;
                };
                match self.spec.kind {
                    WindowKind::Session => {
                        let gap = self.spec.gap_ms.unwrap_or(0);
                        if let Some(pos) = state
                            .sessions
                            .iter()
                            .position(|session| session.end_ms.saturating_add(gap) == fire_at)
                        {
                            let session = state.sessions.remove(pos);
                            closed.push(ClosedWindow {
                                key: key.clone(),
                                window_start: session.start_ms,
                                window_end: session.end_ms,
                                acc: session.acc,
                            });
                        }
                    }
                    WindowKind::Tumble | WindowKind::Hop => {
                        let start = fire_at.saturating_sub(self.spec.size_ms);
                        if let Some(acc) = state.windows.remove(&start) {
                            closed.push(ClosedWindow {
                                key: key.clone(),
                                window_start: start,
                                window_end: fire_at,
                                acc,
                            });
                        }
                    }
                }
                if state.windows.is_empty() && state.sessions.is_empty() {
                    self.keys.remove(&key);
                }
            }
        }
        closed
    }

    fn reindex(&mut self) {
        self.by_end.clear();
        let kind = self.spec.kind;
        let size = self.spec.size_ms;
        let gap = self.spec.gap_ms.unwrap_or(0);
        for (key, state) in &self.keys {
            match kind {
                WindowKind::Tumble | WindowKind::Hop => {
                    for start in state.windows.keys() {
                        self.by_end
                            .entry(start.saturating_add(size))
                            .or_default()
                            .insert(key.clone());
                    }
                }
                WindowKind::Session => {
                    for session in &state.sessions {
                        self.by_end
                            .entry(session.end_ms.saturating_add(gap))
                            .or_default()
                            .insert(key.clone());
                    }
                }
            }
        }
    }
}

impl PartitionClock {
    fn is_active(&self, now: Instant, idle: std::time::Duration) -> bool {
        match self.last_on_time {
            Some(seen) => now.saturating_duration_since(seen) < idle,
            None => false,
        }
    }
}

fn empty_key() -> KeyState {
    KeyState {
        windows: BTreeMap::new(),
        sessions: Vec::new(),
    }
}

fn align_down(ts: i64, size: i64) -> i64 {
    ts - ts.rem_euclid(size)
}

fn intersects(session: &SessionState, t: i64, gap: i64) -> bool {
    t < session.end_ms.saturating_add(gap) && session.start_ms < t.saturating_add(gap)
}

fn unindex(by_end: &mut BTreeMap<i64, BTreeSet<Vec<u8>>>, fire: i64, key: &Vec<u8>) {
    let Some(keys) = by_end.get_mut(&fire) else {
        return;
    };
    keys.remove(key);
    if keys.is_empty() {
        by_end.remove(&fire);
    }
}

fn insert_sorted(sessions: &mut Vec<SessionState>, session: SessionState) {
    let pos = sessions
        .iter()
        .position(|existing| existing.start_ms > session.start_ms)
        .unwrap_or(sessions.len());
    sessions.insert(pos, session);
}

fn fresh_acc(aggs: &[AggSpec]) -> Accumulators {
    Accumulators {
        slots: aggs
            .iter()
            .map(|agg| match agg.kind {
                AggKind::Count => AggState::Count(0),
                AggKind::Sum => AggState::SumI64(0),
                AggKind::Min => AggState::MinI64(0),
                AggKind::Max => AggState::MaxI64(0),
                AggKind::Avg => AggState::Avg { sum: 0.0, count: 0 },
            })
            .collect(),
        present: aggs.iter().map(|agg| agg.kind == AggKind::Count).collect(),
    }
}

fn absorb(
    acc: &mut Accumulators,
    aggs: &[AggSpec],
    values: &[Option<Num>],
    row: &WindowInput,
) -> Result<(), WindowFault> {
    if values.len() != aggs.len() {
        return Err(WindowFault::ValueCount {
            expected: aggs.len(),
            got: values.len(),
        });
    }
    if acc.present.len() != acc.slots.len() {
        acc.present = vec![true; acc.slots.len()];
    }
    for (i, agg) in aggs.iter().enumerate() {
        match agg.kind {
            AggKind::Count => {
                let counts = agg.input.is_none() || values[i].is_some();
                if counts {
                    if let AggState::Count(n) = &mut acc.slots[i] {
                        *n = n.saturating_add(1);
                    }
                }
            }
            AggKind::Sum | AggKind::Min | AggKind::Max | AggKind::Avg => {
                let Some(num) = values[i] else {
                    continue;
                };
                if let Num::F64(v) = num {
                    if !v.is_finite() {
                        return Err(WindowFault::NonFinite {
                            partition: row.partition,
                            offset: row.offset,
                        });
                    }
                }
                apply_num(
                    &mut acc.slots[i],
                    &mut acc.present[i],
                    agg.kind,
                    num,
                    row,
                )?;
            }
        }
    }
    Ok(())
}

fn apply_num(
    slot: &mut AggState,
    present: &mut bool,
    kind: AggKind,
    num: Num,
    row: &WindowInput,
) -> Result<(), WindowFault> {
    if !*present {
        *slot = match (kind, num) {
            (AggKind::Sum, Num::I64(_)) => AggState::SumI64(0),
            (AggKind::Sum, Num::F64(_)) => AggState::SumF64(0.0),
            (AggKind::Min, Num::I64(_)) => AggState::MinI64(0),
            (AggKind::Min, Num::F64(_)) => AggState::MinF64(0.0),
            (AggKind::Max, Num::I64(_)) => AggState::MaxI64(0),
            (AggKind::Max, Num::F64(_)) => AggState::MaxF64(0.0),
            (AggKind::Avg, _) => AggState::Avg { sum: 0.0, count: 0 },
            _ => slot.clone(),
        };
    }
    match (slot, num) {
        (AggState::SumI64(sum), Num::I64(v)) => {
            *sum += i128::from(v);
            if *sum > i128::from(i64::MAX) || *sum < i128::from(i64::MIN) {
                return Err(WindowFault::Overflow {
                    key: row.key.clone().unwrap_or_default(),
                    sum: *sum,
                    partition: row.partition,
                    offset: row.offset,
                });
            }
            *present = true;
        }
        (AggState::SumF64(sum), Num::F64(v)) => {
            *sum += v;
            *present = true;
        }
        (AggState::MinI64(min), Num::I64(v)) => {
            if !*present || v < *min {
                *min = v;
            }
            *present = true;
        }
        (AggState::MinF64(min), Num::F64(v)) => {
            if !*present || v < *min {
                *min = v;
            }
            *present = true;
        }
        (AggState::MaxI64(max), Num::I64(v)) => {
            if !*present || v > *max {
                *max = v;
            }
            *present = true;
        }
        (AggState::MaxF64(max), Num::F64(v)) => {
            if !*present || v > *max {
                *max = v;
            }
            *present = true;
        }
        (AggState::Avg { sum, count }, Num::I64(v)) => {
            *sum += v as f64;
            *count += 1;
            *present = true;
        }
        (AggState::Avg { sum, count }, Num::F64(v)) => {
            *sum += v;
            *count += 1;
            *present = true;
        }
        _ => {
            return Err(WindowFault::TypeMix {
                partition: row.partition,
                offset: row.offset,
            })
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tachyon_core::{AggKind, AggSpec, AggState, WindowKind};

    fn spec(kind: WindowKind, size: i64, slide: Option<i64>, gap: Option<i64>) -> WindowSpecId {
        WindowSpecId {
            input: "orders".into(),
            event_time: "event_time".into(),
            kind,
            size_ms: size,
            slide_ms: slide,
            gap_ms: gap,
            group_columns: vec!["order_id".into()],
            partial: false,
            aggs: vec![
                AggSpec {
                    kind: AggKind::Count,
                    input: None,
                    alias: "n".into(),
                },
                AggSpec {
                    kind: AggKind::Sum,
                    input: Some("amount".into()),
                    alias: "amount".into(),
                },
            ],
        }
    }

    fn row(key: i64, t: i64, partition: i32, amount: i64) -> WindowInput {
        WindowInput {
            key: Some(key.to_le_bytes().to_vec()),
            event_time_ms: Some(t),
            partition,
            offset: t,
            values: vec![None, Some(Num::I64(amount))],
        }
    }

    fn count_of(window: &ClosedWindow) -> i64 {
        match window.acc.slots[0] {
            AggState::Count(n) => n,
            _ => panic!("count"),
        }
    }

    fn sum_of(window: &ClosedWindow) -> i128 {
        match window.acc.slots[1] {
            AggState::SumI64(n) => n,
            _ => panic!("sum"),
        }
    }

    #[test]
    fn tumble_one_second_and_one_minute_close_on_watermark() {
        let t0 = Instant::now();
        let mut second = WindowOperator::new(spec(WindowKind::Tumble, 1_000, None, None), 0, 60_000);
        let closed = second
            .apply(&[row(1, 1_500, 0, 10), row(1, 2_400, 0, 1)], t0)
            .unwrap();
        // El evento de 2400 ms sube el watermark a 2400 y cierra [1000, 2000).
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_start, 1_000);
        assert_eq!(closed[0].window_end, 2_000);
        assert_eq!(count_of(&closed[0]), 1);
        assert_eq!(sum_of(&closed[0]), 10);
        assert_eq!(second.open_windows(), 1);

        let mut minute = WindowOperator::new(spec(WindowKind::Tumble, 60_000, None, None), 0, 60_000);
        let closed = minute
            .apply(
                &[row(7, 30_000, 0, 4), row(7, 90_000, 0, 1)],
                t0,
            )
            .unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_start, 0);
        assert_eq!(closed[0].window_end, 60_000);
        assert_eq!(sum_of(&closed[0]), 4);
    }

    #[test]
    fn hop_puts_each_event_in_two_windows() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Hop, 10_000, Some(5_000), None), 0, 60_000);
        // t=12s cae en [5s,15s) y [10s,20s). Un evento a 20s cierra las dos.
        let closed = op
            .apply(&[row(1, 12_000, 0, 3), row(1, 20_000, 0, 1)], t0)
            .unwrap();
        let starts: Vec<i64> = closed.iter().map(|w| w.window_start).collect();
        assert_eq!(starts, vec![5_000, 10_000]);
        assert!(closed.iter().all(|w| sum_of(w) == 3));
    }

    #[test]
    fn session_merges_inside_the_gap_and_not_on_the_boundary() {
        let t0 = Instant::now();
        let mut merged = WindowOperator::new(spec(WindowKind::Session, 0, None, Some(5_000)), 0, 60_000);
        // 0 y 4 se fusionan. t=9 está justo en end+gap y no entra. El watermark
        // de ese evento cierra la sesión fusionada; t=20 cierra la otra.
        let closed = merged
            .apply(
                &[row(1, 0, 0, 2), row(1, 4_000, 0, 3), row(1, 9_000, 0, 1), row(1, 20_000, 0, 1)],
                t0,
            )
            .unwrap();
        assert_eq!(closed.len(), 2);
        assert_eq!(closed[0].window_start, 0);
        assert_eq!(closed[0].window_end, 4_000);
        assert_eq!(count_of(&closed[0]), 2);
        assert_eq!(sum_of(&closed[0]), 5);
        assert_eq!(closed[1].window_start, 9_000);
        assert_eq!(count_of(&closed[1]), 1);

        let mut apart = WindowOperator::new(spec(WindowKind::Session, 0, None, Some(5_000)), 0, 60_000);
        let closed = apart
            .apply(&[row(1, 0, 0, 1), row(1, 5_000, 0, 1), row(1, 30_000, 0, 1)], t0)
            .unwrap();
        assert_eq!(closed.len(), 2);
        assert_eq!(closed[0].window_end, 0);
        assert_eq!(closed[1].window_start, 5_000);
    }

    #[test]
    fn session_bridges_two_open_intervals() {
        let t0 = Instant::now();
        // lag de 5s: el evento a 10s no dispara la sesión de t=0 (fire a los 6s).
        let mut op = WindowOperator::new(spec(WindowKind::Session, 0, None, Some(6_000)), 5_000, 60_000);
        op.apply(&[row(1, 0, 0, 1), row(1, 10_000, 0, 1)], t0)
            .unwrap();
        assert_eq!(op.open_windows(), 2);
        let closed = op
            .apply(&[row(1, 5_000, 0, 1), row(1, 40_000, 0, 1)], t0)
            .unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_start, 0);
        assert_eq!(closed[0].window_end, 10_000);
        assert_eq!(count_of(&closed[0]), 3);
    }

    #[test]
    fn late_event_does_not_move_the_clock() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Tumble, 1_000, None, None), 0, 60_000);
        op.apply(&[row(1, 5_000, 0, 1)], t0).unwrap();
        assert_eq!(op.instance_watermark_ms(), Some(5_000));
        let closed = op.apply(&[row(1, 100, 0, 9)], t0).unwrap();
        assert!(closed.is_empty());
        assert_eq!(op.instance_watermark_ms(), Some(5_000));
        assert_eq!(op.open_windows(), 1);
    }

    #[test]
    fn an_idle_partition_releases_the_minimum() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Tumble, 1_000, None, None), 0, 1_000);
        op.apply(&[row(1, 500, 0, 4)], t0).unwrap();
        assert_eq!(op.open_windows(), 1);
        // La partición 1 llega después: al cumplirse el idle de la 0, el mínimo
        // sube al reloj de la 1 y cierra [0, 1000).
        op.apply(&[row(1, 10_000, 1, 1)], t0 + Duration::from_millis(500))
            .unwrap();
        assert_eq!(op.instance_watermark_ms(), Some(500));
        let closed = op.on_tick(t0 + Duration::from_millis(1_000));
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].window_end, 1_000);
        assert_eq!(sum_of(&closed[0]), 4);
    }

    #[test]
    fn all_idle_does_not_emit() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Tumble, 1_000, None, None), 0, 1_000);
        op.apply(&[row(1, 100, 0, 1)], t0).unwrap();
        assert_eq!(op.open_windows(), 1);
        let closed = op.on_tick(t0 + Duration::from_secs(30));
        assert!(closed.is_empty());
        assert_eq!(op.open_windows(), 1);
    }

    #[test]
    fn overflow_reverts_the_batch() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Tumble, 60_000, None, None), 0, 60_000);
        op.apply(&[row(1, 1_000, 0, 10)], t0).unwrap();
        let err = op
            .apply(&[row(1, 1_100, 0, i64::MAX)], t0)
            .unwrap_err();
        assert!(matches!(err, WindowFault::Overflow { .. }), "{err}");
        assert_eq!(op.open_windows(), 1);
        let closed = op.apply(&[row(1, 120_000, 0, 1)], t0).unwrap();
        assert_eq!(sum_of(&closed[0]), 10);
    }

    #[test]
    fn restore_rebuilds_the_end_index() {
        let t0 = Instant::now();
        let mut op = WindowOperator::new(spec(WindowKind::Tumble, 1_000, None, None), 0, 60_000);
        op.apply(&[row(1, 100, 0, 6)], t0).unwrap();
        let state = op.freeze_state();
        let watermark = op.instance_watermark_ms();
        let mut restored = WindowOperator::restore(
            spec(WindowKind::Tumble, 1_000, None, None),
            0,
            60_000,
            state,
            watermark,
        );
        assert_eq!(restored.open_windows(), 1);
        let closed = restored.apply(&[row(1, 2_000, 0, 1)], t0).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(sum_of(&closed[0]), 6);
        assert_eq!(closed[0].window_start, 0);
    }
}

fn merge_acc(dst: &mut Accumulators, src: &Accumulators, row: &WindowInput) -> Result<(), WindowFault> {
    if dst.present.len() != dst.slots.len() {
        dst.present = vec![true; dst.slots.len()];
    }
    let src_present: Vec<bool> = if src.present.len() == src.slots.len() {
        src.present.clone()
    } else {
        vec![true; src.slots.len()]
    };
    for i in 0..dst.slots.len() {
        if !src_present[i] {
            continue;
        }
        if !dst.present[i] {
            dst.slots[i] = src.slots[i].clone();
            dst.present[i] = true;
            continue;
        }
        match (&mut dst.slots[i], &src.slots[i]) {
            (AggState::Count(a), AggState::Count(b)) => *a = a.saturating_add(*b),
            (AggState::SumI64(a), AggState::SumI64(b)) => {
                *a += *b;
                if *a > i128::from(i64::MAX) || *a < i128::from(i64::MIN) {
                    return Err(WindowFault::Overflow {
                        key: row.key.clone().unwrap_or_default(),
                        sum: *a,
                        partition: row.partition,
                        offset: row.offset,
                    });
                }
            }
            (AggState::SumF64(a), AggState::SumF64(b)) => *a += *b,
            (AggState::MinI64(a), AggState::MinI64(b)) => *a = (*a).min(*b),
            (AggState::MinF64(a), AggState::MinF64(b)) => *a = a.min(*b),
            (AggState::MaxI64(a), AggState::MaxI64(b)) => *a = (*a).max(*b),
            (AggState::MaxF64(a), AggState::MaxF64(b)) => *a = a.max(*b),
            (
                AggState::Avg { sum, count },
                AggState::Avg {
                    sum: sb,
                    count: cb,
                },
            ) => {
                *sum += *sb;
                *count += *cb;
            }
            _ => {
                return Err(WindowFault::TypeMix {
                    partition: row.partition,
                    offset: row.offset,
                })
            }
        }
    }
    Ok(())
}
