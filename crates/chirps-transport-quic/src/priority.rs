use std::collections::VecDeque;

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
    }

    pub fn dequeue(&mut self) -> Option<ScheduledMessage<T>> {
        let mut visited = 0;
        while visited < self.queues.len() {
            let idx = self.current;

            if self.should_add_quantum[idx] {
                let weight = self.config.weights[idx] as i64;
                self.deficit_counters[idx] += (self.config.quantum_bytes as i64) * weight;
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
                    return Some(msg);
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

    pub fn is_empty(&self, priority: Priority) -> bool {
        self.queues[priority.index()].is_empty()
    }

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

    fn msg(size: usize) -> ScheduledMessage<()> {
        ScheduledMessage::new(Priority::Low, size, ())
    }

    #[test]
    fn dwrr_prefers_high_but_serves_low() {
        let mut sched = PriorityScheduler::new(SchedulerConfig::default());
        for _ in 0..7 {
            sched.enqueue(msg(1024), Priority::High);
        }
        sched.enqueue(msg(1024), Priority::Low);

        for _ in 0..4 {
            let next = sched.dequeue().expect("message expected");
            assert_eq!(next.priority, Priority::High);
        }

        let mut low_seen = false;
        for _ in 0..4 {
            if let Some(next) = sched.dequeue() {
                if next.priority == Priority::Low {
                    low_seen = true;
                    break;
                }
            }
        }

        assert!(
            low_seen,
            "low priority message should be scheduled within a round"
        );
    }

    #[test]
    fn queue_lengths_report_per_priority() {
        let mut sched = PriorityScheduler::new(SchedulerConfig::default());
        sched.enqueue(msg(512), Priority::High);
        sched.enqueue(msg(512), Priority::Normal);
        sched.enqueue(msg(512), Priority::Low);

        assert_eq!(sched.queue_lengths(), [1, 1, 1]);
        let _ = sched.dequeue();
        let _ = sched.dequeue();
        let _ = sched.dequeue();
        assert_eq!(sched.queue_lengths(), [0, 0, 0]);
    }
}
