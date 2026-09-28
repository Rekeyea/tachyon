//! Handoff de una partición entre los consumidores de un mismo proceso.
//!
//! El pass-through no usa este tipo. Una query de ventana comparte un
//! `WindowHandoff` entre los `RdkafkaSource` del input y el operador: un solo
//! mapa `applied`, un solo dueño de cada partición.
//!
//! `rd_kafka_consume_cb` destruye el mensaje actual después de adelantar el
//! app offset. `rd_kafka_yield` solo devuelve a la cola los ops que todavía
//! no se despacharon. Por eso una partición que no se puede leer se pausa
//! (llamada local, no un RPC) y el callback no la copia. Al soltarla se hace
//! seek a `applied`. La consulta de `rd_kafka_committed` y el low watermark
//! corren entre polls, nunca dentro del callback de rebalance.

use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;

/// Qué hacer con una partición asignada, entre dos polls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionAction {
    /// `applied` no la tiene y este `commit_user` todavía no commiteó.
    /// Hay que llamar a `rd_kafka_committed` fuera del lock.
    Query,
    /// No leer. Sigue pausada.
    Hold,
    /// Seek a este offset y reanudar. `None`: sin seek, arranca el grupo.
    Ready(Option<i64>),
}

/// Resultado de una consulta de offset de grupo que sí respondió.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommittedObs {
    /// El broker no tiene offset. Falta sembrar el low watermark.
    NeedLow,
    /// La partición ya está en `applied`. No se rebobina.
    Ready(i64),
}

struct Inner {
    /// Verdadero después del primer snapshot de ventana de este proceso,
    /// o desde el arranque si se restauró un sidecar.
    snapshot_committed: bool,
    applied: BTreeMap<i32, i64>,
    /// Próximo offset copiado a un lote. Puede ir adelante de `applied`
    /// mientras ese lote no pasó por `apply`.
    admitted: BTreeMap<i32, i64>,
    /// Consumidor que copió el high-water. `None` después de un revoke.
    owner: BTreeMap<i32, u64>,
    /// El lector nuevo no pasa de acá hasta que `applied` lo alcance.
    blocked_until: BTreeMap<i32, i64>,
    queried: HashSet<i32>,
    seeded: HashSet<i32>,
    next_consumer: u64,
    fatal: Option<String>,
}

/// Estado compartido del handoff. Un proceso, un input, N consumidores.
pub struct WindowHandoff {
    group: String,
    topic: String,
    commit_user: String,
    checkpoint_id: Option<i64>,
    /// El mapa restaurado es la única membresía válida hasta que este
    /// proceso siembra una partición. No se activa al commitear el primer
    /// snapshot de un arranque vacío.
    restored: bool,
    inner: Mutex<Inner>,
}

impl WindowHandoff {
    /// Arranque sin sidecar de ventana. La consulta de commit ajeno sigue abierta.
    pub fn starting(group: &str, topic: &str, commit_user: &str) -> Self {
        Self::build(group, topic, commit_user, None, false, BTreeMap::new())
    }

    /// Sidecar de ventana restaurado. `applied` es el de ese topic.
    pub fn restored(
        group: &str,
        topic: &str,
        commit_user: &str,
        checkpoint_id: i64,
        applied: BTreeMap<i32, i64>,
    ) -> Self {
        Self::build(
            group,
            topic,
            commit_user,
            Some(checkpoint_id),
            true,
            applied,
        )
    }

    fn build(
        group: &str,
        topic: &str,
        commit_user: &str,
        checkpoint_id: Option<i64>,
        restored: bool,
        applied: BTreeMap<i32, i64>,
    ) -> Self {
        let admitted = applied.clone();
        Self {
            group: group.to_string(),
            topic: topic.to_string(),
            commit_user: commit_user.to_string(),
            checkpoint_id,
            restored,
            inner: Mutex::new(Inner {
                snapshot_committed: restored,
                applied,
                admitted,
                owner: BTreeMap::new(),
                blocked_until: BTreeMap::new(),
                queried: HashSet::new(),
                seeded: HashSet::new(),
                next_consumer: 0,
                fatal: None,
            }),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Id estable dentro de este proceso. Empieza en 1.
    pub fn register_consumer(&self) -> u64 {
        let mut inner = self.lock();
        inner.next_consumer += 1;
        inner.next_consumer
    }

    pub fn classify(&self, consumer: u64, partition: i32) -> Result<PartitionAction, String> {
        let mut inner = self.lock();
        if let Some(fatal) = inner.fatal.clone() {
            return Err(fatal);
        }
        if self.restored
            && !inner.applied.contains_key(&partition)
            && !inner.seeded.contains(&partition)
        {
            let id = self.checkpoint_id.unwrap_or(0);
            let fatal = format!(
                "partición {partition} de '{}' no está en el checkpoint {id} de '{}'",
                self.topic, self.commit_user
            );
            inner.fatal = Some(fatal.clone());
            return Err(fatal);
        }
        if !inner.snapshot_committed
            && !inner.applied.contains_key(&partition)
            && !inner.queried.contains(&partition)
        {
            return Ok(PartitionAction::Query);
        }
        if held(&inner, consumer, partition) {
            return Ok(PartitionAction::Hold);
        }
        // El dueño que ya copió un lote todavía no aplicado retoma en ese
        // high-water. Seek a `applied` (más atrás) lo volvería a leer.
        let applied = inner.applied.get(&partition).copied();
        let admitted = inner.admitted.get(&partition).copied();
        let offset = if inner.owner.get(&partition) == Some(&consumer) {
            match (applied, admitted) {
                (Some(applied), Some(admitted)) => Some(applied.max(admitted)),
                (Some(applied), None) => Some(applied),
                (None, Some(admitted)) => Some(admitted),
                (None, None) => None,
            }
        } else {
            applied
        };
        Ok(PartitionAction::Ready(offset))
    }

    /// El consumidor viejo deja de ser dueño. `blocked_until` queda en el
    /// high-water ya copiado, que puede estar adelante de `applied`.
    pub fn note_revoke(&self, partitions: &[i32]) {
        let mut inner = self.lock();
        for &partition in partitions {
            let until = inner
                .admitted
                .get(&partition)
                .copied()
                .or_else(|| inner.applied.get(&partition).copied());
            if let Some(until) = until {
                let slot = inner.blocked_until.entry(partition).or_insert(until);
                *slot = (*slot).max(until);
            }
            inner.owner.remove(&partition);
        }
    }

    /// Copia el high-water local del poll. No baja un offset ya publicado.
    pub fn flush_admitted(&self, consumer: u64, local: &BTreeMap<i32, i64>) {
        if local.is_empty() {
            return;
        }
        let mut inner = self.lock();
        for (&partition, &next) in local {
            let slot = inner.admitted.entry(partition).or_insert(next);
            *slot = (*slot).max(next);
            inner.owner.insert(partition, consumer);
        }
    }

    /// `committed >= 0` es un commit del grupo y este `commit_user` no tiene
    /// checkpoint de ventana: se sale antes de admitir. Un offset ausente
    /// pide el low watermark. Si `applied` ya tiene la partición, no se toca.
    pub fn observe_committed(
        &self,
        partition: i32,
        committed: Option<i64>,
    ) -> Result<CommittedObs, String> {
        let mut inner = self.lock();
        if let Some(fatal) = inner.fatal.clone() {
            return Err(fatal);
        }
        if let Some(&next) = inner.applied.get(&partition) {
            inner.queried.insert(partition);
            return Ok(CommittedObs::Ready(next));
        }
        if committed.is_some_and(|offset| offset >= 0) {
            let fatal = format!(
                "este commit_user no tiene checkpoint de ventana pero el grupo '{}' ya commiteó la partición {partition}",
                self.group
            );
            inner.fatal = Some(fatal.clone());
            inner.queried.insert(partition);
            return Err(fatal);
        }
        Ok(CommittedObs::NeedLow)
    }

    /// Inserta `applied[p] = low` solo si la clave no existe. Devuelve el
    /// offset que quedó, que no es menor que el que ya había.
    pub fn seed_low(&self, partition: i32, low: i64) -> i64 {
        let mut inner = self.lock();
        inner.queried.insert(partition);
        inner.seeded.insert(partition);
        let value = *inner.applied.entry(partition).or_insert(low);
        inner.admitted.entry(partition).or_insert(value);
        value
    }

    /// El operador fusionó el tracker después de un `apply` que devolvió `Ok`.
    pub fn publish_applied(&self, offsets: &BTreeMap<i32, i64>) {
        let mut inner = self.lock();
        for (&partition, &next) in offsets {
            let reached = {
                let slot = inner.applied.entry(partition).or_insert(next);
                *slot = (*slot).max(next);
                *slot
            };
            if inner
                .blocked_until
                .get(&partition)
                .is_some_and(|until| reached >= *until)
            {
                inner.blocked_until.remove(&partition);
            }
        }
    }

    /// El primer snapshot de ventana de este proceso ya existe.
    pub fn mark_snapshot_committed(&self) {
        self.lock().snapshot_committed = true;
    }

    pub fn applied_map(&self) -> BTreeMap<i32, i64> {
        self.lock().applied.clone()
    }

    pub fn admitted_map(&self) -> BTreeMap<i32, i64> {
        self.lock().admitted.clone()
    }

    /// `Some(until)` si un lector que no es el dueño tiene que esperar.
    pub fn blocked_offset(&self, partition: i32) -> Option<i64> {
        let inner = self.lock();
        let until = inner.blocked_until.get(&partition).copied()?;
        let have = inner.applied.get(&partition).copied().unwrap_or(i64::MIN);
        (have < until).then_some(until)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("lock del handoff")
    }
}

fn held(inner: &Inner, consumer: u64, partition: i32) -> bool {
    if let Some(&until) = inner.blocked_until.get(&partition) {
        let have = inner.applied.get(&partition).copied().unwrap_or(i64::MIN);
        if have < until {
            return true;
        }
    }
    if let Some(&admitted) = inner.admitted.get(&partition) {
        let have = inner.applied.get(&partition).copied().unwrap_or(i64::MIN);
        if admitted > have && inner.owner.get(&partition).is_some_and(|owner| *owner != consumer)
        {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> WindowHandoff {
        WindowHandoff::starting("grupo-orders", "orders", "commit-a")
    }

    #[test]
    fn mixed_lot_waits_for_apply_and_resumes_at_the_new_high_water() {
        let gate = fresh();
        let old = gate.register_consumer();
        let new = gate.register_consumer();
        // La consulta de arranque ya sembró las dos particiones.
        assert_eq!(gate.seed_low(0, 0), 0);
        assert_eq!(gate.seed_low(1, 0), 0);
        // Un lote todavía no aplicado mezcla p=0 (se va a revocar) y q=1.
        let mut local = BTreeMap::new();
        local.insert(0, 5);
        local.insert(1, 3);
        gate.flush_admitted(old, &local);

        // q sigue siendo del dueño viejo: el resume es el high-water copiado,
        // no el `applied` de antes del lote.
        assert_eq!(
            gate.classify(old, 1).unwrap(),
            PartitionAction::Ready(Some(3))
        );
        // El lector nuevo no es el dueño: espera aunque el revoke no haya corrido.
        assert_eq!(gate.classify(new, 0).unwrap(), PartitionAction::Hold);
        gate.note_revoke(&[0]);
        assert_eq!(gate.blocked_offset(0), Some(5));
        assert_eq!(gate.classify(new, 0).unwrap(), PartitionAction::Hold);
        assert_eq!(
            gate.classify(old, 1).unwrap(),
            PartitionAction::Ready(Some(3))
        );

        let mut applied = BTreeMap::new();
        applied.insert(0, 5);
        applied.insert(1, 3);
        gate.publish_applied(&applied);
        assert_eq!(
            gate.classify(new, 0).unwrap(),
            PartitionAction::Ready(Some(5))
        );
        // Un publish más chico no baja el cursor.
        gate.publish_applied(&BTreeMap::from([(0, 1)]));
        assert_eq!(
            gate.classify(new, 0).unwrap(),
            PartitionAction::Ready(Some(5))
        );
        assert!(gate.blocked_offset(0).is_none());
    }

    #[test]
    fn foreign_commit_is_fatal_and_a_missing_offset_seeds_once() {
        let gate = fresh();
        let id = gate.register_consumer();
        assert_eq!(gate.classify(id, 0).unwrap(), PartitionAction::Query);
        let err = gate.observe_committed(0, Some(4)).unwrap_err();
        assert!(err.contains("ya commiteó la partición 0"), "{err}");
        assert!(gate.classify(id, 0).is_err());

        let gate = fresh();
        let id = gate.register_consumer();
        assert_eq!(
            gate.observe_committed(1, None).unwrap(),
            CommittedObs::NeedLow
        );
        assert_eq!(gate.seed_low(1, 0), 0);
        assert_eq!(gate.seed_low(1, 99), 0);
        assert_eq!(
            gate.classify(id, 1).unwrap(),
            PartitionAction::Ready(Some(0))
        );
        gate.publish_applied(&BTreeMap::from([(1, 10)]));
        assert_eq!(gate.seed_low(1, 0), 10);
        assert_eq!(
            gate.observe_committed(1, Some(3)).unwrap(),
            CommittedObs::Ready(10)
        );
        assert_eq!(
            gate.classify(id, 1).unwrap(),
            PartitionAction::Ready(Some(10))
        );
    }

    #[test]
    fn restored_unknown_partition_fails_and_a_later_assign_does_not_query() {
        let gate = WindowHandoff::restored(
            "grupo-orders",
            "orders",
            "commit-a",
            7,
            BTreeMap::from([(0, 12)]),
        );
        let id = gate.register_consumer();
        assert_eq!(
            gate.classify(id, 0).unwrap(),
            PartitionAction::Ready(Some(12))
        );
        let err = gate.classify(id, 1).unwrap_err();
        assert!(
            err.contains("partición 1") && err.contains("checkpoint 7"),
            "{err}"
        );

        let gate = fresh();
        let id = gate.register_consumer();
        gate.seed_low(0, 0);
        gate.mark_snapshot_committed();
        // Una partición nueva, después del primer snapshot de este proceso,
        // no repite la consulta ni se toma como ajena al mapa restaurado.
        assert_eq!(gate.classify(id, 3).unwrap(), PartitionAction::Ready(None));
    }
}
