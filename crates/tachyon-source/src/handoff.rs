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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

/// Pedido del poll al operador: ¿hay una ficha commiteada de esta partición?
pub struct StateRequest {
    pub partition: i32,
    pub ack: tokio::sync::oneshot::Sender<Result<Option<i64>, String>>,
}

struct Member {
    revoking: bool,
    reported: bool,
    assigned: HashSet<i32>,
}

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
    members: HashMap<u64, Member>,
    /// Particiones que este proceso llegó a poseer. El primer assign completo
    /// suelta las que ya no están.
    process_owned: HashSet<i32>,
    pending_release: Vec<i32>,
    next_consumer: u64,
    fatal: Option<String>,
    /// Particiones cuyo consumidor llegó al final del log (PARTITION_EOF de
    /// librdkafka, que ya saltea los markers de transacción). Un mensaje
    /// nuevo la saca.
    at_end: HashSet<i32>,
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
    bus: Mutex<Option<tokio::sync::mpsc::UnboundedSender<StateRequest>>>,
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
        let process_owned = applied.keys().copied().collect();
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
                members: HashMap::new(),
                process_owned,
                pending_release: Vec::new(),
                next_consumer: 0,
                fatal: None,
                at_end: HashSet::new(),
            }),
            bus: Mutex::new(None),
        }
    }

    pub fn bind_state_bus(&self, tx: tokio::sync::mpsc::UnboundedSender<StateRequest>) {
        *self.bus.lock().expect("lock del bus de estado") = Some(tx);
    }

    /// Bloquea hasta que el operador diga si había ficha. `Ok(None)` si este
    /// proceso no tiene operador escuchando.
    pub fn request_adopt(&self, partition: i32) -> Result<Option<i64>, String> {
        let tx = self.bus.lock().expect("lock del bus de estado").clone();
        let Some(tx) = tx else {
            return Ok(None);
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(StateRequest {
            partition,
            ack: ack_tx,
        })
        .map_err(|_| "el operador no recibe adopciones".to_string())?;
        ack_rx
            .blocking_recv()
            .map_err(|_| "el operador cortó la adopción".to_string())?
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Id estable dentro de este proceso. Empieza en 1.
    pub fn register_consumer(&self) -> u64 {
        let mut inner = self.lock();
        inner.next_consumer += 1;
        let id = inner.next_consumer;
        inner.members.insert(
            id,
            Member {
                revoking: false,
                reported: false,
                assigned: HashSet::new(),
            },
        );
        id
    }

    /// Asignación ya estable de un cliente. Cuando todos reportaron y nadie
    /// está en medio de un revoke, las particiones que este proceso ya no
    /// tiene quedan para soltar.
    pub fn note_assignment(&self, consumer: u64, partitions: &[i32]) {
        let mut inner = self.lock();
        let member = inner.members.entry(consumer).or_insert(Member {
            revoking: false,
            reported: false,
            assigned: HashSet::new(),
        });
        member.revoking = false;
        member.reported = true;
        member.assigned = partitions.iter().copied().collect();
        // Un dueño nuevo todavía no sabe si le queda log por leer.
        for partition in partitions {
            inner.at_end.remove(partition);
        }
        recompute_owned(&mut inner);
    }

    /// El consumidor de `partition` llegó al final del log (`true`) o volvió
    /// a recibir un mensaje (`false`).
    pub fn note_end(&self, partition: i32, at_end: bool) {
        let mut inner = self.lock();
        if at_end {
            inner.at_end.insert(partition);
        } else {
            inner.at_end.remove(&partition);
        }
    }

    /// Particiones de este proceso con algo pendiente: el consumidor no llegó
    /// al final del log, o lo copiado todavía no pasó por `apply`. Una
    /// partición así no puede quedar idle: su event time está atrasado por
    /// el drenado, no porque la fuente dejó de mandar. Si el watermark la
    /// ignorara, sus eventos llegarían tarde y se perderían.
    pub fn backlog(&self) -> HashSet<i32> {
        let inner = self.lock();
        let mut out = HashSet::new();
        for member in inner.members.values() {
            for &partition in &member.assigned {
                let unread = !inner.at_end.contains(&partition);
                let unapplied = match (
                    inner.admitted.get(&partition),
                    inner.applied.get(&partition),
                ) {
                    (Some(admitted), Some(applied)) => applied < admitted,
                    (Some(_), None) => true,
                    (None, _) => false,
                };
                if unread || unapplied {
                    out.insert(partition);
                }
            }
        }
        out
    }

    /// El revoke todavía no dice la asignación nueva. No se suelta nada hasta
    /// el assign que le sigue.
    pub fn note_revoking(&self, consumer: u64) {
        let mut inner = self.lock();
        if let Some(member) = inner.members.get_mut(&consumer) {
            member.revoking = true;
            member.assigned.clear();
        }
    }

    pub fn take_released(&self) -> Vec<i32> {
        let mut inner = self.lock();
        let mut out = std::mem::take(&mut inner.pending_release);
        out.sort_unstable();
        out.dedup();
        out
    }

    /// La partición dejó este proceso. El próximo assign vuelve a preguntar
    /// si hay una ficha.
    pub fn forget_partition(&self, partition: i32) {
        let mut inner = self.lock();
        inner.applied.remove(&partition);
        inner.admitted.remove(&partition);
        inner.blocked_until.remove(&partition);
        inner.owner.remove(&partition);
        inner.queried.remove(&partition);
        inner.seeded.remove(&partition);
        inner.process_owned.remove(&partition);
    }

    pub fn note_adopted(&self, partition: i32, offset: i64) {
        let mut inner = self.lock();
        let reached = {
            let slot = inner.applied.entry(partition).or_insert(offset);
            *slot = (*slot).max(offset);
            *slot
        };
        let admitted = inner.admitted.entry(partition).or_insert(reached);
        *admitted = (*admitted).max(reached);
        inner.queried.insert(partition);
        inner.seeded.insert(partition);
        inner.process_owned.insert(partition);
        if inner
            .blocked_until
            .get(&partition)
            .is_some_and(|until| reached >= *until)
        {
            inner.blocked_until.remove(&partition);
        }
    }

    /// No hay ficha. Si este proceso restauró un checkpoint y la partición no
    /// estaba, se sale: no hay estado que adoptar ni low watermark seguro.
    pub fn note_no_ticket(&self, partition: i32) -> Result<(), String> {
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
            inner.queried.insert(partition);
            return Err(fatal);
        }
        Ok(())
    }

    pub fn classify(&self, consumer: u64, partition: i32) -> Result<PartitionAction, String> {
        let inner = self.lock();
        if let Some(fatal) = inner.fatal.clone() {
            return Err(fatal);
        }
        if !inner.applied.contains_key(&partition) && !inner.queried.contains(&partition) {
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

fn recompute_owned(inner: &mut Inner) {
    if inner.members.is_empty() {
        return;
    }
    if inner
        .members
        .values()
        .any(|member| !member.reported || member.revoking)
    {
        return;
    }
    let mut union = HashSet::new();
    for member in inner.members.values() {
        union.extend(member.assigned.iter().copied());
    }
    for partition in inner.process_owned.difference(&union) {
        inner.pending_release.push(*partition);
    }
    inner.process_owned = union;
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
        if admitted > have
            && inner
                .owner
                .get(&partition)
                .is_some_and(|owner| *owner != consumer)
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
        assert_eq!(gate.classify(id, 1).unwrap(), PartitionAction::Query);
        let err = gate.note_no_ticket(1).unwrap_err();
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
        assert_eq!(gate.classify(id, 3).unwrap(), PartitionAction::Query);
    }

    #[test]
    fn a_partition_that_leaves_the_process_is_released_once_assignment_settles() {
        let gate = WindowHandoff::starting("g", "orders", "commit-a");
        let a = gate.register_consumer();
        let b = gate.register_consumer();
        gate.note_assignment(a, &[0, 1]);
        assert!(gate.take_released().is_empty());
        gate.note_assignment(b, &[1]);
        assert!(gate.take_released().is_empty());
        gate.note_revoking(a);
        gate.note_revoking(b);
        assert!(
            gate.take_released().is_empty(),
            "un revoke a medias no suelta las claves"
        );
        gate.note_assignment(a, &[]);
        gate.note_assignment(b, &[1]);
        assert_eq!(gate.take_released(), vec![0]);
    }

    #[test]
    fn backlog_is_unread_or_unapplied_log() {
        let gate = WindowHandoff::starting("g", "t", "u");
        let a = gate.register_consumer();
        gate.note_assignment(a, &[0, 1, 2]);
        // Recién asignadas: no se sabe si queda log.
        assert_eq!(gate.backlog(), [0, 1, 2].into_iter().collect());
        gate.note_end(0, true);
        gate.note_end(1, true);
        gate.flush_admitted(a, &BTreeMap::from([(1, 50)]));
        // La 0 está al final y vacía; la 1 copió 50 que todavía no se aplicaron.
        assert_eq!(gate.backlog(), [1, 2].into_iter().collect());
        gate.publish_applied(&BTreeMap::from([(1, 50)]));
        assert_eq!(gate.backlog(), [2].into_iter().collect());
        // Un mensaje nuevo la vuelve a poner en backlog.
        gate.note_end(0, false);
        assert_eq!(gate.backlog(), [0, 2].into_iter().collect());
        // Reasignar olvida el fin: el dueño nuevo no sabe cuánto le queda.
        gate.note_assignment(a, &[1]);
        assert_eq!(gate.backlog(), [1].into_iter().collect());
    }
}
