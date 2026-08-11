use crate::MessageProfile;
use std::collections::VecDeque;

/// FIFO queues ordered by the profile priority Control > Durable > Ephemeral.
#[derive(Debug)]
pub struct PriorityQueue<T> {
    control: VecDeque<T>,
    durable: VecDeque<T>,
    ephemeral: VecDeque<T>,
}

impl<T> Default for PriorityQueue<T> {
    fn default() -> Self {
        Self {
            control: VecDeque::new(),
            durable: VecDeque::new(),
            ephemeral: VecDeque::new(),
        }
    }
}

impl<T> PriorityQueue<T> {
    pub fn push(&mut self, profile: MessageProfile, value: T) {
        match profile {
            MessageProfile::Control => self.control.push_back(value),
            MessageProfile::Durable => self.durable.push_back(value),
            MessageProfile::Ephemeral => self.ephemeral.push_back(value),
        }
    }

    pub fn pop(&mut self) -> Option<(MessageProfile, T)> {
        self.control
            .pop_front()
            .map(|value| (MessageProfile::Control, value))
            .or_else(|| {
                self.durable
                    .pop_front()
                    .map(|value| (MessageProfile::Durable, value))
            })
            .or_else(|| {
                self.ephemeral
                    .pop_front()
                    .map(|value| (MessageProfile::Ephemeral, value))
            })
    }

    pub fn len(&self) -> usize {
        self.control.len() + self.durable.len() + self.ephemeral.len()
    }

    pub fn is_empty(&self) -> bool {
        self.control.is_empty() && self.durable.is_empty() && self.ephemeral.is_empty()
    }
}
