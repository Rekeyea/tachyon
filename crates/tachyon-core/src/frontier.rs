//! Offsets comiteables cuando varios carriles escriben la misma partición.
//!
//! Con un carril por consumidor, cada batch llega al writer con los rangos de
//! offsets que cubre, `[desde, hasta)` por partición. Los rangos de una
//! partición se encadenan: el primero de un consumidor arranca donde arrancó
//! a leerla (el checkpoint, lo que ya copió el dueño anterior o el primer
//! offset) y cada uno sigue donde terminó el anterior, así que cubren también
//! los offsets sin mensaje (markers de transacción, compaction).
//!
//! Cuando una partición cambia de consumidor dentro del proceso, el carril
//! nuevo arranca donde dejó de copiar el viejo, pero sus batches pueden
//! llegar al writer antes que los últimos del viejo. Comitear el máximo
//! visto declararía escritas filas que todavía están en el carril viejo: si
//! el proceso cae ahí, se pierden. La frontera solo avanza por rangos
//! contiguos desde el origen; lo que llega adelantado espera el hueco.

use std::collections::BTreeMap;

/// Un tramo de offsets de una partición: `[from, to)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetRange {
    pub partition: i32,
    pub from: i64,
    pub to: i64,
}

/// Próximo offset comiteable por partición.
#[derive(Debug, Clone, Default)]
pub struct Frontier {
    next: BTreeMap<i32, i64>,
    /// Tramos adelantados (desde -> hasta), por partición.
    ahead: BTreeMap<i32, BTreeMap<i64, i64>>,
}

impl Frontier {
    /// Arranca en los offsets del último checkpoint (partición -> próximo).
    pub fn new(resume: &BTreeMap<i32, i64>) -> Self {
        Self {
            next: resume.clone(),
            ahead: BTreeMap::new(),
        }
    }

    /// `origin` da el primer offset que el proceso leyó de una partición sin
    /// checkpoint. Tiene que estar definido antes de que exista un rango de
    /// esa partición (lo fija quien la copió primero).
    pub fn cover(&mut self, range: OffsetRange, origin: impl FnOnce(i32) -> Option<i64>) {
        if range.to <= range.from {
            return;
        }
        let partition = range.partition;
        let next = match self.next.get(&partition) {
            Some(&next) => next,
            None => match origin(partition) {
                Some(start) => {
                    self.next.insert(partition, start);
                    start
                }
                None => {
                    // Sin origen no se sabe qué hay antes: queda esperando.
                    self.ahead
                        .entry(partition)
                        .or_default()
                        .entry(range.from)
                        .and_modify(|to| *to = (*to).max(range.to))
                        .or_insert(range.to);
                    return;
                }
            },
        };
        if range.from > next {
            self.ahead
                .entry(partition)
                .or_default()
                .entry(range.from)
                .and_modify(|to| *to = (*to).max(range.to))
                .or_insert(range.to);
            return;
        }
        let mut reached = next.max(range.to);
        if let Some(ahead) = self.ahead.get_mut(&partition) {
            while let Some((&from, &to)) = ahead.first_key_value() {
                if from > reached {
                    break;
                }
                reached = reached.max(to);
                ahead.remove(&from);
            }
            if ahead.is_empty() {
                self.ahead.remove(&partition);
            }
        }
        self.next.insert(partition, reached);
    }

    /// Próximo offset comiteable de cada partición.
    pub fn offsets(&self) -> &BTreeMap<i32, i64> {
        &self.next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(partition: i32, from: i64, to: i64) -> OffsetRange {
        OffsetRange { partition, from, to }
    }

    #[test]
    fn contiguous_ranges_advance() {
        let mut f = Frontier::new(&BTreeMap::new());
        f.cover(r(0, 0, 10), |_| Some(0));
        f.cover(r(0, 10, 25), |_| None);
        assert_eq!(f.offsets().get(&0), Some(&25));
    }

    #[test]
    fn a_range_that_overtakes_waits_for_the_gap() {
        // La partición pasó del carril A al B en 100. B escribió [100, 150)
        // antes de que A terminara [60, 100): comitear 150 perdería 60..100.
        let mut f = Frontier::new(&BTreeMap::from([(3, 60)]));
        f.cover(r(3, 100, 150), |_| None);
        assert_eq!(f.offsets().get(&3), Some(&60));
        f.cover(r(3, 150, 170), |_| None);
        assert_eq!(f.offsets().get(&3), Some(&60));
        f.cover(r(3, 60, 100), |_| None);
        assert_eq!(f.offsets().get(&3), Some(&170));
    }

    #[test]
    fn origin_seeds_a_partition_without_checkpoint() {
        let mut f = Frontier::new(&BTreeMap::new());
        f.cover(r(1, 500, 600), |_| None);
        assert_eq!(f.offsets().get(&1), None, "sin origen no se comitea nada");
        f.cover(r(1, 400, 500), |_| Some(400));
        assert_eq!(f.offsets().get(&1), Some(&600));
    }

    #[test]
    fn partitions_are_independent_and_empty_ranges_are_ignored() {
        let mut f = Frontier::new(&BTreeMap::from([(0, 5), (1, 7)]));
        f.cover(r(0, 5, 5), |_| None);
        f.cover(r(1, 7, 9), |_| None);
        assert_eq!(f.offsets(), &BTreeMap::from([(0, 5), (1, 9)]));
    }

    #[test]
    fn a_range_behind_the_frontier_does_not_move_it_back() {
        let mut f = Frontier::new(&BTreeMap::from([(0, 50)]));
        f.cover(r(0, 10, 30), |_| None);
        assert_eq!(f.offsets().get(&0), Some(&50));
        f.cover(r(0, 40, 80), |_| None);
        assert_eq!(f.offsets().get(&0), Some(&80));
    }
}
