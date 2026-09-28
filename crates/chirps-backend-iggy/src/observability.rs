//! Bounded, secret-free Durable metrics and structured events.

use alopex_chirps_core::durable::{
    DurableEvent, DurableMetricLabels, DurableTraceContext, ResourceEpoch,
};
use std::collections::VecDeque;
use thiserror::Error;

const MAX_METRIC_SERIES: usize = 512;
const MAX_EVENT_CAPACITY: usize = 1_024;

/// Hard limits for in-process Durable observability state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservabilityLimits {
    metric_series: usize,
    event_capacity: usize,
}

impl ObservabilityLimits {
    /// Validates finite, non-zero observability capacities.
    pub fn new(metric_series: usize, event_capacity: usize) -> Result<Self, ObservabilityError> {
        if metric_series == 0 || metric_series > MAX_METRIC_SERIES {
            return Err(ObservabilityError::InvalidMetricSeriesLimit);
        }
        if event_capacity == 0 || event_capacity > MAX_EVENT_CAPACITY {
            return Err(ObservabilityError::InvalidEventCapacity);
        }
        Ok(Self {
            metric_series,
            event_capacity,
        })
    }
}

/// One bounded label set and its monotonic counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricSeries {
    labels: DurableMetricLabels,
    count: u64,
}

/// Finite health projection of the in-process observability buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservabilitySnapshot {
    metric_series: usize,
    metric_series_limit: usize,
    retained_events: usize,
    event_capacity: usize,
    dropped_events: u64,
}

/// One owned, finite observation of a running Durable backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityReport {
    snapshot: ObservabilitySnapshot,
    metric_series: Vec<MetricSeries>,
    events: Vec<DurableEvent>,
}

impl ObservabilityReport {
    /// Returns bounded-buffer health without correlation identity.
    #[must_use]
    pub fn snapshot(&self) -> ObservabilitySnapshot {
        self.snapshot
    }

    /// Returns the complete finite metric series at observation time.
    #[must_use]
    pub fn metric_series(&self) -> &[MetricSeries] {
        &self.metric_series
    }

    /// Returns retained structured events in oldest-to-newest order.
    #[must_use]
    pub fn events(&self) -> &[DurableEvent] {
        &self.events
    }

    /// Iterates retained events for one partition/resource incarnation.
    pub fn partition_events(
        &self,
        partition: u32,
        resource_epoch: ResourceEpoch,
    ) -> impl Iterator<Item = &DurableEvent> {
        self.events.iter().filter(move |event| {
            matches!(
                event.trace(),
                DurableTraceContext::Partition {
                    partition: event_partition,
                    resource_epoch: event_resource,
                    ..
                } if event_partition == partition && event_resource == resource_epoch
            )
        })
    }
}

impl ObservabilitySnapshot {
    /// Returns the number of occupied metric series.
    #[must_use]
    pub const fn metric_series(self) -> usize {
        self.metric_series
    }

    /// Returns the configured metric-series hard limit.
    #[must_use]
    pub const fn metric_series_limit(self) -> usize {
        self.metric_series_limit
    }

    /// Returns the number of retained structured events.
    #[must_use]
    pub const fn retained_events(self) -> usize {
        self.retained_events
    }

    /// Returns the configured structured-event hard limit.
    #[must_use]
    pub const fn event_capacity(self) -> usize {
        self.event_capacity
    }

    /// Returns the saturating count of evicted events.
    #[must_use]
    pub const fn dropped_events(self) -> u64 {
        self.dropped_events
    }
}

impl MetricSeries {
    /// Returns the provider-neutral, low-cardinality labels.
    #[must_use]
    pub const fn labels(self) -> DurableMetricLabels {
        self.labels
    }

    /// Returns the number of observations for this exact label set.
    #[must_use]
    pub const fn count(self) -> u64 {
        self.count
    }
}

/// A finite in-process sink for Durable metrics and structured events.
#[derive(Debug)]
pub struct BoundedObservability {
    limits: ObservabilityLimits,
    metric_series: Vec<MetricSeries>,
    events: VecDeque<DurableEvent>,
    dropped_event_count: u64,
}

impl BoundedObservability {
    /// Creates an empty sink with already validated hard limits.
    #[must_use]
    pub fn new(limits: ObservabilityLimits) -> Self {
        Self {
            limits,
            metric_series: Vec::with_capacity(limits.metric_series),
            events: VecDeque::with_capacity(limits.event_capacity),
            dropped_event_count: 0,
        }
    }

    /// Increments one finite metric series, rejecting new cardinality at the
    /// configured hard limit.
    pub fn record_metric(&mut self, labels: DurableMetricLabels) -> Result<(), ObservabilityError> {
        if let Some(series) = self
            .metric_series
            .iter_mut()
            .find(|series| series.labels == labels)
        {
            series.count = series
                .count
                .checked_add(1)
                .ok_or(ObservabilityError::CounterExhausted)?;
            return Ok(());
        }
        if self.metric_series.len() == self.limits.metric_series {
            return Err(ObservabilityError::MetricSeriesCapacity);
        }
        self.metric_series.push(MetricSeries { labels, count: 1 });
        Ok(())
    }

    /// Appends one structured event, evicting the oldest event at capacity.
    pub fn record_event(&mut self, event: DurableEvent) {
        if self.events.len() == self.limits.event_capacity {
            self.events.pop_front();
            self.dropped_event_count = self.dropped_event_count.saturating_add(1);
        }
        self.events.push_back(event);
    }

    /// Returns the complete finite metric series snapshot.
    #[must_use]
    pub fn metric_series(&self) -> &[MetricSeries] {
        &self.metric_series
    }

    /// Returns retained structured events in oldest-to-newest order.
    #[must_use]
    pub const fn events(&self) -> &VecDeque<DurableEvent> {
        &self.events
    }

    /// Returns the saturating number of events evicted at the hard limit.
    #[must_use]
    pub const fn dropped_event_count(&self) -> u64 {
        self.dropped_event_count
    }

    /// Returns bounded buffer health without exposing any trace identity.
    #[must_use]
    pub fn snapshot(&self) -> ObservabilitySnapshot {
        ObservabilitySnapshot {
            metric_series: self.metric_series.len(),
            metric_series_limit: self.limits.metric_series,
            retained_events: self.events.len(),
            event_capacity: self.limits.event_capacity,
            dropped_events: self.dropped_event_count,
        }
    }

    /// Copies the finite buffers for read-only inspection by the public
    /// facade. The report remains evidence only and cannot affect outcomes.
    #[must_use]
    pub fn report(&self) -> ObservabilityReport {
        ObservabilityReport {
            snapshot: self.snapshot(),
            metric_series: self.metric_series.clone(),
            events: self.events.iter().copied().collect(),
        }
    }

    /// Iterates the retained events for one partition/resource incarnation.
    ///
    /// Owner epochs intentionally remain in each returned event so a caller
    /// can reconstruct rebinds without placing correlation identity in metric
    /// labels. The bounded FIFO remains evidence only; operation outcomes stay
    /// authoritative in their owning components.
    pub fn partition_events(
        &self,
        partition: u32,
        resource_epoch: ResourceEpoch,
    ) -> impl Iterator<Item = &DurableEvent> {
        self.events.iter().filter(move |event| {
            matches!(
                event.trace(),
                DurableTraceContext::Partition {
                    partition: event_partition,
                    resource_epoch: event_resource,
                    ..
                } if event_partition == partition && event_resource == resource_epoch
            )
        })
    }
}

/// A rejected observability capacity or update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ObservabilityError {
    /// The metric-series limit is zero or exceeds the implementation maximum.
    #[error("metric-series limit is outside the supported range")]
    InvalidMetricSeriesLimit,
    /// The event capacity is zero or exceeds the implementation maximum.
    #[error("event capacity is outside the supported range")]
    InvalidEventCapacity,
    /// A new metric label set would exceed the configured finite cardinality.
    #[error("metric-series capacity is exhausted")]
    MetricSeriesCapacity,
    /// One metric counter reached its exact maximum value.
    #[error("metric counter is exhausted")]
    CounterExhausted,
}

#[cfg(test)]
mod tests {
    use super::{BoundedObservability, ObservabilityError, ObservabilityLimits};
    use alopex_chirps_core::durable::{
        DurableEvent, DurableEventKind, DurableMetricLabels, DurableTraceContext, MetricBoundary,
        MetricFailureStage, MetricOperation, MetricOutcome, ResourceEpoch, ResourceId,
    };

    fn labels(outcome: MetricOutcome) -> DurableMetricLabels {
        DurableMetricLabels::new(
            MetricOperation::Checkpoint,
            MetricBoundary::CheckpointInstall,
            MetricFailureStage::FileSync,
            outcome,
        )
    }

    #[test]
    fn v07_task_4_5_metrics_are_finite_and_correlation_never_becomes_a_label() {
        let mut observability = BoundedObservability::new(ObservabilityLimits::new(2, 2).unwrap());
        observability
            .record_metric(labels(MetricOutcome::Success))
            .unwrap();
        observability
            .record_metric(labels(MetricOutcome::Success))
            .unwrap();
        observability
            .record_metric(labels(MetricOutcome::Unknown))
            .unwrap();
        assert_eq!(observability.metric_series().len(), 2);
        assert_eq!(observability.metric_series()[0].count(), 2);
        assert_eq!(
            observability.record_metric(labels(MetricOutcome::Gap)),
            Err(ObservabilityError::MetricSeriesCapacity)
        );

        let high_cardinality = DurableTraceContext::partition(
            77,
            u64::MAX - 1,
            ResourceEpoch::new(ResourceId::from_bytes([0x77; 16]), u64::MAX),
        );
        observability.record_event(DurableEvent::new(DurableEventKind::Fault, high_cardinality));
        assert_eq!(observability.events()[0].trace(), high_cardinality);
        assert!(!format!("{:?}", observability.metric_series()).contains("77777777"));
    }

    #[test]
    fn v07_task_4_5_event_buffer_is_bounded_and_reports_drops() {
        let mut observability = BoundedObservability::new(ObservabilityLimits::new(1, 2).unwrap());
        for fill in [0x11, 0x22, 0x33] {
            observability.record_event(DurableEvent::new(
                DurableEventKind::Recovery,
                DurableTraceContext::partition(
                    u32::from(fill),
                    u64::from(fill),
                    ResourceEpoch::new(ResourceId::from_bytes([fill; 16]), u64::from(fill)),
                ),
            ));
        }
        assert_eq!(observability.events().len(), 2);
        assert_eq!(observability.dropped_event_count(), 1);
        assert_eq!(
            observability.events()[0].trace(),
            DurableTraceContext::partition(
                0x22,
                0x22,
                ResourceEpoch::new(ResourceId::from_bytes([0x22; 16]), 0x22),
            )
        );
    }

    #[test]
    fn v07_task_4_5_limits_reject_zero_and_implementation_excess() {
        assert_eq!(
            ObservabilityLimits::new(0, 1),
            Err(ObservabilityError::InvalidMetricSeriesLimit)
        );
        assert_eq!(
            ObservabilityLimits::new(1, 0),
            Err(ObservabilityError::InvalidEventCapacity)
        );
        assert_eq!(
            ObservabilityLimits::new(513, 1),
            Err(ObservabilityError::InvalidMetricSeriesLimit)
        );
        assert_eq!(
            ObservabilityLimits::new(1, 1_025),
            Err(ObservabilityError::InvalidEventCapacity)
        );
    }

    #[test]
    fn v07_task_6_14_snapshot_stays_finite_after_event_eviction() {
        let mut observability = BoundedObservability::new(ObservabilityLimits::new(2, 2).unwrap());
        observability
            .record_metric(labels(MetricOutcome::Success))
            .unwrap();
        for owner_epoch in 1..=3 {
            observability.record_event(DurableEvent::new(
                DurableEventKind::Recovery,
                DurableTraceContext::partition(
                    7,
                    owner_epoch,
                    ResourceEpoch::new(ResourceId::from_bytes([0x44; 16]), 9),
                ),
            ));
        }

        let snapshot = observability.snapshot();
        assert_eq!(snapshot.metric_series(), 1);
        assert_eq!(snapshot.metric_series_limit(), 2);
        assert_eq!(snapshot.retained_events(), 2);
        assert_eq!(snapshot.event_capacity(), 2);
        assert_eq!(snapshot.dropped_events(), 1);
    }

    #[test]
    fn v07_task_6_14_partition_chain_crosses_owner_epoch_but_not_resource_epoch() {
        let resource = ResourceEpoch::new(ResourceId::from_bytes([0x55; 16]), 11);
        let other_resource = ResourceEpoch::new(ResourceId::from_bytes([0x55; 16]), 12);
        let mut observability = BoundedObservability::new(ObservabilityLimits::new(1, 8).unwrap());
        for (kind, owner_epoch) in [
            (DurableEventKind::Fault, 1),
            (DurableEventKind::Recovery, 2),
            (DurableEventKind::Redelivery, 2),
            (DurableEventKind::Checkpoint, 2),
            (DurableEventKind::SessionRebind, 3),
        ] {
            observability.record_event(DurableEvent::new(
                kind,
                DurableTraceContext::partition(3, owner_epoch, resource),
            ));
        }
        observability.record_event(DurableEvent::new(
            DurableEventKind::Fault,
            DurableTraceContext::partition(3, 4, other_resource),
        ));

        let chain: Vec<_> = observability
            .partition_events(3, resource)
            .map(|event| event.kind())
            .collect();
        assert_eq!(
            chain,
            [
                DurableEventKind::Fault,
                DurableEventKind::Recovery,
                DurableEventKind::Redelivery,
                DurableEventKind::Checkpoint,
                DurableEventKind::SessionRebind,
            ]
        );
    }

    #[test]
    fn v07_task_6_14_rendered_contract_has_no_free_form_security_fields() {
        let resource = ResourceEpoch::new(ResourceId::from_bytes([0x66; 16]), u64::MAX);
        let mut observability = BoundedObservability::new(ObservabilityLimits::new(1, 1).unwrap());
        observability
            .record_metric(labels(MetricOutcome::RecoveryRequired))
            .unwrap();
        observability.record_event(DurableEvent::new(
            DurableEventKind::Recovery,
            DurableTraceContext::partition(u32::MAX, u64::MAX, resource),
        ));

        let rendered = format!(
            "{:?}{:?}",
            observability.metric_series(),
            observability.events()
        )
        .to_ascii_lowercase();
        for forbidden in [
            "message_id",
            "target",
            "payload",
            "credential",
            "password",
            "token",
            "permission",
        ] {
            assert!(!rendered.contains(forbidden));
        }
    }
}
