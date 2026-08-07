use super::{GroupHandle, GroupId, MultiRaftError, RaftStorageFactory};
use crate::raft::{
    ChirpsRaftTransport, RaftConfig, RaftFramePayload, RaftMessage, RaftMetricsCollector, RaftNode,
};
use alopex_chirps_raft_storage::SnapshotCompletionHook;
use alopex_chirps_wire::frame::Frame;
use alopex_chirps_wire::node_id::NodeId;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

/// Owns the lifecycle registry for all Raft groups on one Chirps node.
pub struct MultiRaftManager<F> {
    transport: Arc<ChirpsRaftTransport>,
    factory: Arc<F>,
    raft_config: RaftConfig,
    config: MultiRaftConfig,
    lifecycle: Mutex<()>,
    groups: RwLock<HashMap<GroupId, Arc<GroupHandle>>>,
    metrics_collector: RwLock<Option<Arc<RaftMetricsCollector>>>,
}

/// Operational limits for one Multi-Raft manager.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiRaftConfig {
    /// Maximum number of simultaneously managed groups.
    pub max_groups: usize,
}

impl Default for MultiRaftConfig {
    fn default() -> Self {
        Self { max_groups: 100 }
    }
}

#[derive(Debug)]
pub struct RoutedRaftResponse {
    pub source: u64,
    pub destination: u64,
    pub correlation_id: u64,
    pub message: RaftMessage,
}

#[derive(Debug)]
pub struct GroupTickResult {
    pub group_id: GroupId,
    pub result: Result<(), MultiRaftError>,
}

impl<F> MultiRaftManager<F>
where
    F: RaftStorageFactory,
{
    pub fn new(
        transport: Arc<ChirpsRaftTransport>,
        factory: Arc<F>,
        raft_config: RaftConfig,
    ) -> Self {
        Self::new_with_config(transport, factory, raft_config, MultiRaftConfig::default())
    }

    pub fn new_with_config(
        transport: Arc<ChirpsRaftTransport>,
        factory: Arc<F>,
        mut raft_config: RaftConfig,
        config: MultiRaftConfig,
    ) -> Self {
        raft_config.node_id = transport.node_id();
        Self {
            transport,
            factory,
            raft_config,
            config,
            lifecycle: Mutex::new(()),
            groups: RwLock::new(HashMap::new()),
            metrics_collector: RwLock::new(None),
        }
    }

    /// Registers one collector for all existing and subsequently-created groups.
    pub async fn set_metrics_collector(&self, collector: Arc<RaftMetricsCollector>) {
        let _lifecycle = self.lifecycle.lock().await;
        let snapshot_hook: Arc<dyn SnapshotCompletionHook> = collector.clone();
        self.factory.set_snapshot_completion_hook(snapshot_hook);
        *self
            .metrics_collector
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&collector));
        for group in self.groups_read().values() {
            group.set_metrics_collector(Arc::clone(&collector));
        }
        collector.set_groups_total(self.groups_count());
    }

    pub async fn create_group(
        &self,
        group_id: GroupId,
        initial_members: BTreeSet<u64>,
        state_machine: F::StateMachine,
    ) -> Result<(), MultiRaftError> {
        self.create_group_inner(group_id, Some(initial_members), state_machine)
            .await
    }

    /// Creates a routable local replica without initializing a second cluster.
    ///
    /// A seed node must later add this replica as a learner, wait for catch-up,
    /// and promote it through [`GroupHandle::change_membership`].
    pub async fn create_group_uninitialized(
        &self,
        group_id: GroupId,
        state_machine: F::StateMachine,
    ) -> Result<(), MultiRaftError> {
        self.create_group_inner(group_id, None, state_machine).await
    }

    async fn create_group_inner(
        &self,
        group_id: GroupId,
        initial_members: Option<BTreeSet<u64>>,
        state_machine: F::StateMachine,
    ) -> Result<(), MultiRaftError> {
        let _lifecycle = self.lifecycle.lock().await;
        if self.groups_read().contains_key(&group_id) {
            return Err(MultiRaftError::GroupAlreadyExists { group_id });
        }
        if self.groups_count() >= self.config.max_groups {
            return Err(MultiRaftError::GroupLimitExceeded {
                limit: self.config.max_groups,
            });
        }

        let transaction = self.factory.begin_storage(group_id, state_machine).await?;
        let storage = transaction.storage();
        let transport = Arc::new(self.transport.fork_for_group(group_id));
        let mut config = self.raft_config.clone();
        config.group_id = group_id;

        let node = match RaftNode::new(
            config,
            ChirpsRaftTransport::factory(Arc::clone(&transport)),
            storage.clone(),
            storage,
            transport,
        )
        .await
        {
            Ok(node) => Arc::new(node),
            Err(error) => {
                let cleanup = transaction.abort().await;
                return Err(node_initialization_error(group_id, error, cleanup));
            }
        };

        if let Some(initial_members) = initial_members
            && let Err(error) = node.initialize(initial_members).await
        {
            let shutdown = node.shutdown().await;
            drop(node);
            let cleanup = transaction.abort().await;
            let mut reason = error.to_string();
            if let Err(shutdown_error) = shutdown {
                reason.push_str(&format!("; node rollback failed: {shutdown_error}"));
            }
            if let Err(cleanup_error) = cleanup {
                reason.push_str(&format!("; storage rollback failed: {cleanup_error}"));
            }
            return Err(MultiRaftError::NodeInitialization { group_id, reason });
        }

        // Commit the storage allocation before making the replica routable.
        // The lifecycle lock makes the following map insertion infallible with
        // respect to duplicate creation.
        drop(transaction.commit());
        let handle = Arc::new(GroupHandle::new(group_id, Arc::clone(&node)));
        if let Some(collector) = self
            .metrics_collector
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            handle.set_metrics_collector(collector);
        }
        let replaced = self.groups_write().insert(group_id, handle);
        if let Some(previous) = replaced {
            let inserted = self
                .groups_write()
                .insert(group_id, previous)
                .expect("newly inserted group must still be present");
            let _ = node.shutdown().await;
            drop(inserted);
            drop(node);
            return Err(MultiRaftError::GroupAlreadyExists { group_id });
        }
        if let Some(collector) = self
            .metrics_collector
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            collector.set_groups_total(self.groups_count());
        }
        Ok(())
    }

    pub fn get_group(&self, group_id: GroupId) -> Option<Arc<GroupHandle>> {
        self.groups_read().get(&group_id).cloned()
    }

    pub fn list_groups(&self) -> Vec<GroupId> {
        let mut groups = self.groups_read().keys().copied().collect::<Vec<_>>();
        groups.sort_unstable_by_key(|group_id| group_id.0);
        groups
    }

    pub fn groups_count(&self) -> usize {
        self.groups_read().len()
    }

    pub async fn remove_group(&self, group_id: GroupId) -> Result<bool, MultiRaftError> {
        let _lifecycle = self.lifecycle.lock().await;
        let Some(group) = self.groups_read().get(&group_id).cloned() else {
            return Ok(false);
        };

        group.shutdown().await?;
        self.groups_write().remove(&group_id);
        if let Some(collector) = self
            .metrics_collector
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            collector.remove_group(group_id);
            collector.set_groups_total(self.groups_count());
        }
        Ok(true)
    }

    pub async fn route_message(
        &self,
        source: u64,
        destination: u64,
        payload: RaftFramePayload,
    ) -> Result<RoutedRaftResponse, MultiRaftError> {
        let group_id = payload.message.group_id();
        if is_response(&payload.message) {
            return Err(MultiRaftError::InvalidTransportRoute {
                reason: "Raft responses must be delivered through dispatch_message".to_owned(),
            });
        }
        if destination != self.transport.node_id() {
            return Err(MultiRaftError::Routing {
                group_id,
                reason: format!(
                    "destination mismatch: expected {}, got {destination}",
                    self.transport.node_id()
                ),
            });
        }
        let group = self
            .get_group(group_id)
            .ok_or(MultiRaftError::UnknownGroup { group_id })?;
        group.record_received(&payload.message);
        let correlation_id = payload.correlation_id;
        let message = group.handle_message(payload).await?;
        if message.group_id() != group_id {
            return Err(MultiRaftError::Routing {
                group_id,
                reason: "Raft response group mismatch".to_owned(),
            });
        }
        Ok(RoutedRaftResponse {
            source: destination,
            destination: source,
            correlation_id,
            message,
        })
    }

    /// Sends a routed response through the same group transport that produced it.
    /// Successful sends are included in the per-group message metrics.
    pub async fn send_routed_response(
        &self,
        response: RoutedRaftResponse,
    ) -> Result<(), MultiRaftError> {
        if response.source != self.transport.node_id() {
            return Err(MultiRaftError::InvalidTransportRoute {
                reason: format!(
                    "response source mismatch: expected {}, got {}",
                    self.transport.node_id(),
                    response.source
                ),
            });
        }
        let group_id = response.message.group_id();
        let group = self
            .get_group(group_id)
            .ok_or(MultiRaftError::UnknownGroup { group_id })?;
        group
            .send_response(
                response.destination,
                response.correlation_id,
                response.message,
            )
            .await
    }

    /// Decodes and routes one request frame for compatibility callers.
    ///
    /// Wire node IDs are accepted only in the canonical representation used by
    /// `ChirpsRaftTransport`: eight zero bytes followed by the big-endian Raft
    /// node ID. Non-Raft frames, malformed payloads, and non-canonical node IDs
    /// are rejected before any group is looked up or mutated. Actual receive
    /// loops must use [`Self::dispatch_frame`] so correlated responses wake the
    /// correct pending group RPC.
    pub async fn route_frame(
        &self,
        source: NodeId,
        destination: NodeId,
        frame: Frame,
    ) -> Result<RoutedRaftResponse, MultiRaftError> {
        let source =
            decode_wire_node_id(source).ok_or_else(|| MultiRaftError::InvalidTransportRoute {
                reason: "source NodeId is not a canonical Raft node ID".to_owned(),
            })?;
        let destination = decode_wire_node_id(destination).ok_or_else(|| {
            MultiRaftError::InvalidTransportRoute {
                reason: "destination NodeId is not a canonical Raft node ID".to_owned(),
            }
        })?;
        let payload = ChirpsRaftTransport::decode_frame(frame).ok_or_else(|| {
            MultiRaftError::InvalidTransportRoute {
                reason: "frame is not a valid group-consistent Raft frame".to_owned(),
            }
        })?;
        self.route_message(source, destination, payload).await
    }

    /// Dispatches a frame from an actual receive loop.
    ///
    /// Requests return `Some(response)` for the caller to encode and send.
    /// Responses are validated against the per-group pending RPC table, wake
    /// exactly one waiter, and return `None`.
    pub async fn dispatch_frame(
        &self,
        source: NodeId,
        destination: NodeId,
        frame: Frame,
    ) -> Result<Option<RoutedRaftResponse>, MultiRaftError> {
        let source =
            decode_wire_node_id(source).ok_or_else(|| MultiRaftError::InvalidTransportRoute {
                reason: "source NodeId is not a canonical Raft node ID".to_owned(),
            })?;
        let destination = decode_wire_node_id(destination).ok_or_else(|| {
            MultiRaftError::InvalidTransportRoute {
                reason: "destination NodeId is not a canonical Raft node ID".to_owned(),
            }
        })?;
        let payload = ChirpsRaftTransport::decode_frame(frame).ok_or_else(|| {
            MultiRaftError::InvalidTransportRoute {
                reason: "frame is not a valid group-consistent Raft frame".to_owned(),
            }
        })?;
        self.dispatch_message(source, destination, payload).await
    }

    /// Dispatches one decoded request or correlated response.
    pub async fn dispatch_message(
        &self,
        source: u64,
        destination: u64,
        payload: RaftFramePayload,
    ) -> Result<Option<RoutedRaftResponse>, MultiRaftError> {
        let group_id = payload.message.group_id();
        if destination != self.transport.node_id() {
            return Err(MultiRaftError::Routing {
                group_id,
                reason: format!(
                    "destination mismatch: expected {}, got {destination}",
                    self.transport.node_id()
                ),
            });
        }
        let correlation_id = payload.correlation_id;
        let group = self
            .get_group(group_id)
            .ok_or(MultiRaftError::UnknownGroup { group_id })?;
        let Some(message) = group.dispatch_message(source, payload).await? else {
            return Ok(None);
        };
        if message.group_id() != group_id {
            return Err(MultiRaftError::Routing {
                group_id,
                reason: "Raft response group mismatch".to_owned(),
            });
        }
        Ok(Some(RoutedRaftResponse {
            source: destination,
            destination: source,
            correlation_id,
            message,
        }))
    }

    pub async fn tick_all(&self) -> Vec<GroupTickResult> {
        let groups = self
            .groups_read()
            .iter()
            .map(|(group_id, group)| (*group_id, Arc::clone(group)))
            .collect::<Vec<_>>();
        let mut tasks = groups
            .into_iter()
            .map(|(group_id, group)| (group_id, tokio::spawn(async move { group.tick().await })))
            .collect::<Vec<_>>();
        let mut results = Vec::with_capacity(tasks.len());
        for (group_id, task) in tasks.drain(..) {
            let result = match task.await {
                Ok(result) => result,
                Err(error) => Err(MultiRaftError::Routing {
                    group_id,
                    reason: format!("tick task failed: {error}"),
                }),
            };
            results.push(GroupTickResult { group_id, result });
        }
        results.sort_unstable_by_key(|result| result.group_id.0);
        results
    }

    /// Triggers one independent heartbeat task per group without making a
    /// slow group stall heartbeats for unrelated groups. A per-group gate
    /// suppresses overlapping ticks while preserving bounded task count.
    pub fn tick_all_background(&self) {
        for group in self.groups_read().values() {
            group.spawn_tick();
        }
    }

    pub async fn shutdown_all(&self) -> Result<(), MultiRaftError> {
        for group_id in self.list_groups() {
            self.remove_group(group_id).await?;
        }
        Ok(())
    }

    fn groups_read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<GroupId, Arc<GroupHandle>>> {
        self.groups
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn groups_write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<GroupId, Arc<GroupHandle>>> {
        self.groups
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn decode_wire_node_id(node_id: NodeId) -> Option<u64> {
    let bytes = node_id.as_bytes();
    if bytes[..8] != [0; 8] {
        return None;
    }
    Some(u64::from_be_bytes(bytes[8..].try_into().ok()?))
}

fn is_response(message: &RaftMessage) -> bool {
    message.is_response()
}

fn node_initialization_error(
    group_id: GroupId,
    error: crate::raft::RaftError,
    cleanup: Result<(), MultiRaftError>,
) -> MultiRaftError {
    let mut reason = error.to_string();
    if let Err(cleanup_error) = cleanup {
        reason.push_str(&format!("; storage rollback failed: {cleanup_error}"));
    }
    MultiRaftError::NodeInitialization { group_id, reason }
}
