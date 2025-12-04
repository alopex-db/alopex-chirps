use std::collections::VecDeque;

use tracing::debug;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Priority {
    High = 0,
    Normal = 1,
    Low = 2,
}

impl Priority {
    #[inline]
    fn index(self) -> usize {
        self as usize
    }

    pub(crate) fn from_index(idx: usize) -> Self {
        match idx {
            0 => Priority::High,
            1 => Priority::Normal,
            _ => Priority::Low,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SchedulerConfig {
    pub weights: [u32; 3],
    pub quantum_bytes: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            weights: [4, 2, 1],
            quantum_bytes: 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScheduledMessage<T = ()> {
    pub priority: Priority,
    pub size_bytes: usize,
    pub payload: T,
}

impl<T> ScheduledMessage<T> {
    pub fn new(priority: Priority, size_bytes: usize, payload: T) -> Self {
        ScheduledMessage {
            priority,
            size_bytes,
            payload,
        }
    }
}

pub(crate) struct PriorityScheduler<T = ()> {
    queues: [VecDeque<ScheduledMessage<T>>; 3],
    deficit_counters: [i64; 3],
    should_add_quantum: [bool; 3],
    current: usize,
    config: SchedulerConfig,
}

impl<T> PriorityScheduler<T> {
    pub fn new(config: SchedulerConfig) -> Self {
        PriorityScheduler {
            queues: [VecDeque::new(), VecDeque::new(), VecDeque::new()],
            deficit_counters: [0, 0, 0],
            should_add_quantum: [true, true, true],
            current: 0,
            config,
        }
    }

    pub fn enqueue(&mut self, mut msg: ScheduledMessage<T>, priority: Priority) {
        let idx = priority.index();
        msg.priority = priority;
        self.queues[idx].push_back(msg);
        debug!(
            ?priority,
            size_bytes = self.queues[idx].back().map(|m| m.size_bytes).unwrap_or(0),
            queue_len = self.queues[idx].len(),
            "priority_enqueue"
        );
    }

    pub fn dequeue(&mut self) -> Option<ScheduledMessage<T>> {
        let mut visited = 0;
        while visited < self.queues.len() {
            let idx = self.current;

            if self.should_add_quantum[idx] {
                let weight = self.config.weights[idx] as i64;
                self.deficit_counters[idx] += (self.config.quantum_bytes as i64) * weight;
                debug!(
                    priority = ?Priority::from_index(idx),
                    added_quantum = self.config.quantum_bytes,
                    weight,
                    deficit = self.deficit_counters[idx],
                    "priority_add_quantum"
                );
            }

            if let Some(front) = self.queues[idx].front() {
                let needed = front.size_bytes as i64;
                if self.deficit_counters[idx] >= needed {
                    let mut msg = self.queues[idx].pop_front().expect("front existed");
                    self.deficit_counters[idx] -= needed;
                    msg.priority = Priority::from_index(idx);

                    let has_more = !self.queues[idx].is_empty() && self.deficit_counters[idx] > 0;
                    self.should_add_quantum[idx] = !has_more;
                    if self.should_add_quantum[idx] {
                        self.current = (idx + 1) % self.queues.len();
                    }
                    debug!(
                        priority = ?Priority::from_index(idx),
                        size_bytes = needed,
                        deficit_remaining = self.deficit_counters[idx],
                        queue_remaining = self.queues[idx].len(),
                        "priority_dequeue"
                    );
                    return Some(msg);
                } else {
                    debug!(
                        priority = ?Priority::from_index(idx),
                        needed,
                        deficit = self.deficit_counters[idx],
                        "priority_deficit_insufficient"
                    );
                }
            } else {
                self.deficit_counters[idx] = 0;
            }

            self.should_add_quantum[idx] = true;
            self.current = (idx + 1) % self.queues.len();
            visited += 1;
        }

        None
    }

    #[allow(dead_code)]
    pub fn is_empty(&self, priority: Priority) -> bool {
        self.queues[priority.index()].is_empty()
    }

    #[allow(dead_code)]
    pub fn queue_lengths(&self) -> [usize; 3] {
        [
            self.queues[0].len(),
            self.queues[1].len(),
            self.queues[2].len(),
        ]
    }
}

impl<T> Default for PriorityScheduler<T> {
    fn default() -> Self {
        PriorityScheduler::new(SchedulerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamKind;
    use tracing_test::traced_test;

    fn msg(size: usize, priority: Priority) -> ScheduledMessage<()> {
        ScheduledMessage::new(priority, size, ())
    }

    #[test]
    fn stream_kind_priority_mapping_matches_design() {
        assert_eq!(StreamKind::Control.priority(), Priority::High);
        assert_eq!(StreamKind::Raft.priority(), Priority::High);
        assert_eq!(StreamKind::Gossip.priority(), Priority::Normal);
        assert_eq!(StreamKind::RaftSnapshot.priority(), Priority::Normal);
        assert_eq!(StreamKind::User.priority(), Priority::Low);
    }

    #[test]
    fn dwrr_prefers_high_but_serves_low() {
        let mut sched = PriorityScheduler::new(SchedulerConfig::default());
        for _ in 0..7 {
            sched.enqueue(msg(1024, Priority::High), Priority::High);
        }
        sched.enqueue(msg(1024, Priority::Low), Priority::Low);

        for _ in 0..4 {
            let next = sched.dequeue().expect("message expected");
            assert_eq!(next.priority, Priority::High);
        }

        let mut low_seen = false;
        for _ in 0..4 {
            if let Some(next) = sched.dequeue()
                && next.priority == Priority::Low
            {
                low_seen = true;
                break;
            }
        }

        assert!(
            low_seen,
            "low priority message should be scheduled within a round"
        );
    }

    #[test]
    fn dwrr_respects_configurable_weights() {
        let config = SchedulerConfig {
            weights: [1, 1, 10],
            quantum_bytes: 512,
        };
        let mut sched = PriorityScheduler::new(config);
        sched.enqueue(msg(1024, Priority::High), Priority::High);
        sched.enqueue(msg(512, Priority::Low), Priority::Low);
        sched.enqueue(msg(512, Priority::Low), Priority::Low);

        let first = sched.dequeue().expect("message expected");
        assert_eq!(
            first.priority,
            Priority::Low,
            "heavy weight should allow low priority to win when deficit insufficient for high"
        );
    }

    #[test]
    fn queue_lengths_report_per_priority_and_empty_handling() {
        let mut sched = PriorityScheduler::new(SchedulerConfig::default());
        sched.enqueue(msg(512, Priority::High), Priority::High);
        sched.enqueue(msg(512, Priority::Normal), Priority::Normal);
        sched.enqueue(msg(512, Priority::Low), Priority::Low);

        assert_eq!(sched.queue_lengths(), [1, 1, 1]);
        assert_eq!(sched.dequeue().unwrap().priority, Priority::High);
        assert_eq!(sched.dequeue().unwrap().priority, Priority::Normal);
        assert_eq!(sched.dequeue().unwrap().priority, Priority::Low);
        assert_eq!(sched.queue_lengths(), [0, 0, 0]);
        assert!(
            sched.dequeue().is_none(),
            "empty scheduler should return None"
        );
    }

    #[traced_test]
    #[test]
    fn emits_tracing_for_enqueue_and_dequeue() {
        let mut sched = PriorityScheduler::new(SchedulerConfig::default());
        sched.enqueue(msg(256, Priority::High), Priority::High);
        sched.enqueue(msg(256, Priority::Low), Priority::Low);

        assert!(
            logs_contain("priority_enqueue"),
            "enqueue should emit tracing debug logs"
        );

        let first = sched.dequeue().expect("message expected");
        assert_eq!(first.priority, Priority::High);
        let _ = sched.dequeue();

        assert!(
            logs_contain("priority_dequeue"),
            "dequeue should emit tracing debug logs"
        );
        assert!(
            logs_contain("priority=\"High\"") || logs_contain("priority=High"),
            "log should include selected priority"
        );
    }
}
