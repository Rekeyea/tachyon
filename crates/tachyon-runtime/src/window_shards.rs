//! El operador de ventanas repartido en shards que aplican en paralelo.
//!
//! Un solo `WindowOperator` aplicaba todas las filas de la instancia en un
//! hilo: con backlog era el techo del camino de ventana (un core entero
//! mientras los otros esperaban). Acá hay K operadores y cada uno es dueño
//! fijo de las particiones de Kafka con `partición % K`. Como toda clave vive
//! en una sola partición (la clave única de particionado), el estado de una
//! clave está en un solo shard, y que una partición cambie de consumidor
//! dentro del proceso no mueve estado entre shards.
//!
//! El watermark sigue siendo uno solo para la instancia: el mínimo sobre
//! todas las particiones activas o con backlog, igual que con un operador.
//! Durante un batch cada shard aplica con ese watermark como techo (una fila
//! más vieja es tarde, como antes). Al terminar, el wrapper junta los
//! mínimos de los shards, sube el de la instancia y cierra en todos hasta ahí.
//! Frente al operador único, el watermark avanza por batch y no por fila:
//! una ventana puede cerrar un batch más tarde, nunca antes.
//!
//! El checkpoint no cambia de formato: `freeze_state` junta las claves de
//! todos los shards y `restore` las reparte por la partición de cada clave.

use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

use tachyon_core::{KeyState, OperatorState, WindowSpecId};

use crate::window::{ClosedWindow, KeyKind, LocalMin, WindowFault, WindowInput, WindowOperator};

/// Filas por debajo de las cuales un batch se aplica en el hilo que llama:
/// abrir hilos cuesta más que aplicar pocas filas.
const PARALLEL_MIN_ROWS: usize = 4_096;

pub struct ShardedWindow {
    shards: Vec<WindowOperator>,
    spec: WindowSpecId,
    watermark: Option<i64>,
}

impl ShardedWindow {
    pub fn new(
        spec: WindowSpecId,
        lag_ms: i64,
        idle_ms: i64,
        key_kind: KeyKind,
        shards: usize,
    ) -> Self {
        let shards = (0..shards.max(1))
            .map(|_| WindowOperator::new(spec.clone(), lag_ms, idle_ms, key_kind))
            .collect();
        Self {
            shards,
            spec,
            watermark: None,
        }
    }

    /// Estado de un sidecar: cada clave va al shard de su partición.
    pub fn restore(
        spec: WindowSpecId,
        lag_ms: i64,
        idle_ms: i64,
        state: OperatorState,
        instance_watermark_ms: Option<i64>,
        key_kind: KeyKind,
        shards: usize,
    ) -> Result<Self, String> {
        let count = shards.max(1);
        let mut split: Vec<BTreeMap<Vec<u8>, KeyState>> = vec![BTreeMap::new(); count];
        for (key, value) in state.keys {
            split[shard_of(value.partition, count)].insert(key, value);
        }
        let shards = split
            .into_iter()
            .map(|keys| {
                WindowOperator::restore(
                    spec.clone(),
                    lag_ms,
                    idle_ms,
                    OperatorState { keys },
                    instance_watermark_ms,
                    key_kind,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            shards,
            spec,
            watermark: instance_watermark_ms,
        })
    }

    pub fn spec(&self) -> &WindowSpecId {
        &self.spec
    }

    pub fn instance_watermark_ms(&self) -> Option<i64> {
        self.watermark
    }

    fn shard_mut(&mut self, partition: i32) -> &mut WindowOperator {
        let count = self.shards.len();
        &mut self.shards[shard_of(partition, count)]
    }

    pub fn is_released(&self, partition: i32) -> bool {
        self.shards[shard_of(partition, self.shards.len())].is_released(partition)
    }

    pub fn release_partition(&mut self, partition: i32) {
        self.shard_mut(partition).release_partition(partition);
    }

    pub fn set_backlog(&mut self, backlog: HashSet<i32>) {
        let count = self.shards.len();
        let mut split: Vec<HashSet<i32>> = vec![HashSet::new(); count];
        for partition in backlog {
            split[shard_of(partition, count)].insert(partition);
        }
        for (shard, part) in self.shards.iter_mut().zip(split) {
            shard.set_backlog(part);
        }
    }

    pub fn activate_restored(&mut self, progress: &BTreeMap<i32, Option<i64>>, now: Instant) {
        let count = self.shards.len();
        let mut split: Vec<BTreeMap<i32, Option<i64>>> = vec![BTreeMap::new(); count];
        for (partition, max) in progress {
            split[shard_of(*partition, count)].insert(*partition, *max);
        }
        for (shard, part) in self.shards.iter_mut().zip(split) {
            shard.activate_restored(&part, now);
        }
    }

    pub fn adopt_partition(
        &mut self,
        partition: i32,
        keys: BTreeMap<Vec<u8>, KeyState>,
        max_event_time_ms: Option<i64>,
        now: Instant,
    ) -> Result<Vec<ClosedWindow>, String> {
        let watermark = self.watermark;
        let shard = self.shard_mut(partition);
        shard.set_cap(watermark);
        let mut closed = shard.adopt_partition(partition, keys, max_event_time_ms, now)?;
        closed.extend(self.advance(now));
        Ok(closed)
    }

    /// Aplica un batch: cada fila en el shard de su partición, en paralelo.
    /// Una falla en un shard deja a todos envenenados (el batch quedó a
    /// medias en los otros): cada llamada siguiente devuelve esa falla.
    pub fn apply(
        &mut self,
        rows: Vec<WindowInput>,
        now: Instant,
    ) -> Result<Vec<ClosedWindow>, WindowFault> {
        if let Some(fault) = self.shards.iter().find_map(|shard| shard.poison().cloned()) {
            return Err(fault);
        }
        let count = self.shards.len();
        for shard in &mut self.shards {
            shard.set_cap(self.watermark);
        }
        let mut closed = Vec::new();
        if count == 1 || rows.len() < PARALLEL_MIN_ROWS {
            let mut split: Vec<Vec<WindowInput>> = (0..count).map(|_| Vec::new()).collect();
            for row in rows {
                split[shard_of(row.partition, count)].push(row);
            }
            for (shard, part) in self.shards.iter_mut().zip(split) {
                if part.is_empty() {
                    continue;
                }
                match shard.apply(&part, now) {
                    Ok(done) => closed.extend(done),
                    Err(fault) => return Err(self.poison_all(fault)),
                }
            }
        } else {
            let mut split: Vec<Vec<WindowInput>> = (0..count)
                .map(|_| Vec::with_capacity(rows.len() / count + 1))
                .collect();
            for row in rows {
                split[shard_of(row.partition, count)].push(row);
            }
            let results: Vec<Result<Vec<ClosedWindow>, WindowFault>> =
                std::thread::scope(|scope| {
                    let handles: Vec<_> = self
                        .shards
                        .iter_mut()
                        .zip(split)
                        .map(|(shard, part)| {
                            scope.spawn(move || {
                                if part.is_empty() {
                                    Ok(Vec::new())
                                } else {
                                    shard.apply(&part, now)
                                }
                            })
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|handle| handle.join().expect("un shard de ventana entró en pánico"))
                        .collect()
                });
            for result in results {
                match result {
                    Ok(done) => closed.extend(done),
                    Err(fault) => return Err(self.poison_all(fault)),
                }
            }
        }
        closed.extend(self.advance(now));
        Ok(closed)
    }

    /// Recalcula particiones idle y cierra si el mínimo de la instancia subió.
    pub fn on_tick(&mut self, now: Instant) -> Vec<ClosedWindow> {
        if self.shards.iter().any(|shard| shard.poison().is_some()) {
            return Vec::new();
        }
        self.advance(now)
    }

    /// Sube el watermark de la instancia al mínimo de los shards y cierra
    /// en todos hasta ahí. Un shard con backlog sin eventos frena todo; uno
    /// sin particiones activas no frena a nadie.
    fn advance(&mut self, now: Instant) -> Vec<ClosedWindow> {
        let mut min: Option<i64> = None;
        for shard in &mut self.shards {
            match shard.local_min(now) {
                LocalMin::Hold => return Vec::new(),
                LocalMin::Idle => {}
                LocalMin::At(value) => min = Some(min.map_or(value, |m| m.min(value))),
            }
        }
        let Some(min) = min else {
            return Vec::new();
        };
        let watermark = self.watermark.map_or(min, |current| current.max(min));
        self.watermark = Some(watermark);
        let mut closed = Vec::new();
        for shard in &mut self.shards {
            shard.set_cap(Some(watermark));
            closed.extend(shard.raise_to(watermark));
        }
        closed
    }

    fn poison_all(&mut self, fault: WindowFault) -> WindowFault {
        for shard in &mut self.shards {
            shard.poison_with(fault.clone());
        }
        fault
    }

    pub fn partition_progress(&self) -> BTreeMap<i32, Option<i64>> {
        self.shards
            .iter()
            .flat_map(|shard| shard.partition_progress())
            .collect()
    }

    pub fn freeze_state(&self) -> OperatorState {
        let mut keys = BTreeMap::new();
        for shard in &self.shards {
            keys.extend(shard.freeze_state().keys);
        }
        OperatorState { keys }
    }

    pub fn open_windows(&self) -> usize {
        self.shards.iter().map(WindowOperator::open_windows).sum()
    }
}

fn shard_of(partition: i32, shards: usize) -> usize {
    partition.rem_euclid(shards as i32) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::window::{Num, WinKey};
    use tachyon_core::{AggKind, AggSpec, WindowKind};

    fn spec() -> WindowSpecId {
        WindowSpecId {
            input: "orders".into(),
            event_time: "t".into(),
            partial: false,
            kind: WindowKind::Tumble,
            size_ms: 1_000,
            slide_ms: None,
            gap_ms: None,
            group_columns: vec!["k".into()],
            aggs: vec![
                AggSpec {
                    kind: AggKind::Count,
                    input: None,
                    alias: "n".into(),
                    project: None,
                },
                AggSpec {
                    kind: AggKind::Sum,
                    input: Some("v".into()),
                    alias: "s".into(),
                    project: None,
                },
            ],
        }
    }

    fn row(key: i64, t: i64, partition: i32, offset: i64, v: i64) -> WindowInput {
        WindowInput {
            key: Some(WinKey::I64(key)),
            event_time_ms: Some(t),
            partition,
            offset,
            values: vec![None, Some(Num::I64(v))].into(),
        }
    }

    /// (clave, inicio) -> (COUNT, SUM) de todas las ventanas cerradas.
    fn summarize(closed: &[ClosedWindow]) -> BTreeMap<(Vec<u8>, i64), (i64, i128)> {
        use tachyon_core::AggState;
        closed
            .iter()
            .map(|window| {
                let count = match window.acc.slots[0] {
                    AggState::Count(n) => n,
                    _ => panic!("count"),
                };
                let sum = match window.acc.slots[1] {
                    AggState::SumI64(s) => s,
                    _ => 0,
                };
                ((window.key.to_bytes(), window.window_start), (count, sum))
            })
            .collect()
    }

    #[test]
    fn shards_close_the_same_windows_as_one_operator() {
        // 8 particiones, 64 claves (clave % 8 == partición), event time
        // monótono por partición y particiones que avanzan a distinto ritmo.
        let t0 = Instant::now();
        let mut single = WindowOperator::new(spec(), 50, 60_000, KeyKind::I64);
        let mut sharded = ShardedWindow::new(spec(), 50, 60_000, KeyKind::I64, 4);
        let mut clocks = [0i64; 8];
        let mut offsets = [0i64; 8];
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let (mut out_single, mut out_sharded) = (Vec::new(), Vec::new());
        for _batch in 0..60 {
            let mut rows = Vec::new();
            for _ in 0..(if x % 3 == 0 { 9_000 } else { 700 }) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let partition = (x % 8) as i32;
                clocks[partition as usize] += (x >> 8) as i64 % (3 + partition as i64);
                offsets[partition as usize] += 1;
                let key = ((x >> 20) % 8) as i64 * 8 + partition as i64;
                rows.push(row(
                    key,
                    clocks[partition as usize],
                    partition,
                    offsets[partition as usize],
                    (x >> 30) as i64 % 100,
                ));
            }
            out_single.extend(single.apply(&rows, t0).unwrap());
            out_sharded.extend(sharded.apply(rows, t0).unwrap());
        }
        // Un evento muy adelante en cada partición cierra todo lo abierto.
        let far: Vec<WindowInput> = (0..8)
            .map(|p| row(p as i64, 10_000_000, p, 1_000_000, 0))
            .collect();
        out_single.extend(single.apply(&far, t0).unwrap());
        out_sharded.extend(sharded.apply(far, t0).unwrap());
        let a = summarize(&out_single);
        let b = summarize(&out_sharded);
        assert!(
            a.len() > 100,
            "el test tiene que cerrar ventanas: {}",
            a.len()
        );
        assert_eq!(a, b);
        assert_eq!(single.open_windows(), sharded.open_windows());
    }

    #[test]
    fn the_instance_watermark_is_the_minimum_across_shards() {
        let t0 = Instant::now();
        let mut op = ShardedWindow::new(spec(), 0, 60_000, KeyKind::I64, 2);
        // La partición 0 (shard 0) va por 5000; la 1 (shard 1) por 1500.
        op.apply(vec![row(0, 5_000, 0, 1, 1), row(1, 1_500, 1, 1, 1)], t0)
            .unwrap();
        assert_eq!(op.instance_watermark_ms(), Some(1_500));
        // [1000, 2000) de la clave 1 sigue abierta hasta que la 1 pase 2000.
        assert_eq!(op.open_windows(), 2);
        let closed = op.apply(vec![row(1, 2_500, 1, 2, 1)], t0).unwrap();
        assert_eq!(closed.len(), 1, "cierra [1000, 2000) de la clave 1");
        assert_eq!(op.instance_watermark_ms(), Some(2_500));
    }

    #[test]
    fn freeze_and_restore_keep_every_key_in_its_shard() {
        let t0 = Instant::now();
        let mut op = ShardedWindow::new(spec(), 0, 60_000, KeyKind::I64, 3);
        let rows: Vec<WindowInput> = (0..9).map(|p| row(p as i64, 100, p, 1, 1)).collect();
        op.apply(rows, t0).unwrap();
        let state = op.freeze_state();
        assert_eq!(state.keys.len(), 9);
        let back = ShardedWindow::restore(
            spec(),
            0,
            60_000,
            state.clone(),
            op.instance_watermark_ms(),
            KeyKind::I64,
            3,
        )
        .unwrap();
        assert_eq!(back.freeze_state(), state);
        assert_eq!(back.open_windows(), 9);
    }
}
