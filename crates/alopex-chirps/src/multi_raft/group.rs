use super::{GroupId, MultiRaftError};
use crate::raft::{BasicNode, ChirpsNodeId};
use crate::raft::{RaftFramePayload, RaftMessage, RaftNode};
use openraft::metrics::RaftMetrics;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{OnceCell, RwLock, RwLockReadGuard};

/// Clone-safe access point for one managed Raft group.
///
/// Every asynchronous operation holds a shared lifecycle permit. Shutdown
/// closes admission first, then waits for all existing permits to drain.
pub struct GroupHandle {
    group_id: GroupId,
    node: Arc<RaftNode>,
    lifecycle: GroupLifecycle,
}

impl GroupHandle {
    pub(crate) fn new(group_id: GroupId, node: Arc<RaftNode>) -> Self {
        Self {
            group_id,
            node,
            lifecycle: GroupLifecycle::new(),
        }
    }

    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub fn is_accepting(&self) -> bool {
        self.lifecycle.is_accepting()
    }

    /// Returns a clone of OpenRaft's latest observation for local diagnostics
    /// and cross-group isolation checks.
    pub fn metrics(&self) -> RaftMetrics<ChirpsNodeId, BasicNode> {
        self.node.metrics()
    }

    pub async fn propose(&self, command: Vec<u8>) -> Result<Vec<u8>, MultiRaftError> {
        let _permit = self.lifecycle.acquire(self.group_id).await?;
        self.node
            .propose(command)
            .await
            .map_err(|error| MultiRaftError::Routing {
                group_id: self.group_id,
                reason: error.to_string(),
            })
    }

    pub async fn handle_message(
        &self,
        payload: RaftFramePayload,
    ) -> Result<RaftMessage, MultiRaftError> {
        let _permit = self.lifecycle.acquire(self.group_id).await?;
        self.node
            .handle_message(payload)
            .await
            .map_err(|error| MultiRaftError::Routing {
                group_id: self.group_id,
                reason: error.to_string(),
            })
    }

    /// Dispatches one inbound group frame while holding the lifecycle permit.
    ///
    /// Requests are handled and return a response for the caller to send.
    /// Correlated responses wake the group's pending RPC and return `None`.
    pub(crate) async fn dispatch_message(
        &self,
        source: ChirpsNodeId,
        payload: RaftFramePayload,
    ) -> Result<Option<RaftMessage>, MultiRaftError> {
        let request = self
            .node
            .transport
            .consume_incoming_from(source, payload)
            .await
            .map_err(|error| MultiRaftError::Routing {
                group_id: self.group_id,
                reason: error.to_string(),
            })?;
        let Some(request) = request else {
            // A valid response completes work that already owns a lifecycle
            // permit. It must remain deliverable after shutdown closes
            // admission, otherwise drain can deadlock waiting for that work.
            return Ok(None);
        };
        let _permit = self.lifecycle.acquire(self.group_id).await?;
        self.node
            .handle_message(request)
            .await
            .map(Some)
            .map_err(|error| MultiRaftError::Routing {
                group_id: self.group_id,
                reason: error.to_string(),
            })
    }

    /// Adds and catches up an already-published uninitialized replica.
    pub async fn add_learner(
        &self,
        node_id: ChirpsNodeId,
        node: BasicNode,
    ) -> Result<(), MultiRaftError> {
        let _permit = self.lifecycle.acquire(self.group_id).await?;
        self.node
            .add_learner(node_id, node)
            .await
            .map_err(|error| MultiRaftError::Membership {
                group_id: self.group_id,
                reason: error.to_string(),
            })
    }

    /// Promotes caught-up learners to the supplied common voter set.
    pub async fn change_membership(
        &self,
        members: std::collections::BTreeSet<ChirpsNodeId>,
    ) -> Result<(), MultiRaftError> {
        let _permit = self.lifecycle.acquire(self.group_id).await?;
        self.node
            .change_membership(members)
            .await
            .map_err(|error| MultiRaftError::Membership {
                group_id: self.group_id,
                reason: error.to_string(),
            })
    }

    pub async fn tick(&self) -> Result<(), MultiRaftError> {
        let _permit = self.lifecycle.acquire(self.group_id).await?;
        self.node
            .tick()
            .await
            .map_err(|error| MultiRaftError::Routing {
                group_id: self.group_id,
                reason: error.to_string(),
            })
    }

    pub async fn shutdown(&self) -> Result<(), MultiRaftError> {
        let group_id = self.group_id;
        self.lifecycle.close_admission();
        self.lifecycle
            .shutdown_with(async {
                // Existing lifecycle-held operations may need more than one
                // outbound RPC after receiving a valid response. Keep the
                // transport open until those operations drain, then reject
                // new background RPCs and cancel anything residual.
                self.node.close_transport_admission();
                self.node.cancel_pending_transport_rpcs();
                self.node
                    .shutdown()
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(|reason| MultiRaftError::Shutdown { group_id, reason })
    }
}

struct GroupLifecycle {
    accepting: AtomicBool,
    operation_gate: RwLock<()>,
    shutdown_result: OnceCell<Result<(), String>>,
}

impl GroupLifecycle {
    fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            operation_gate: RwLock::new(()),
            shutdown_result: OnceCell::new(),
        }
    }

    fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    fn close_admission(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    async fn acquire(&self, group_id: GroupId) -> Result<RwLockReadGuard<'_, ()>, MultiRaftError> {
        if !self.is_accepting() {
            return Err(MultiRaftError::GroupUnavailable { group_id });
        }
        let permit = self.operation_gate.read().await;
        if !self.is_accepting() {
            return Err(MultiRaftError::GroupUnavailable { group_id });
        }
        Ok(permit)
    }

    async fn shutdown_with<F>(&self, shutdown: F) -> Result<(), String>
    where
        F: Future<Output = Result<(), String>>,
    {
        self.close_admission();
        self.shutdown_result
            .get_or_init(|| async {
                let _drained = self.operation_gate.write().await;
                shutdown.await
            })
            .await
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn shutdown_drains_operations_rejects_new_work_and_is_idempotent() {
        let lifecycle = Arc::new(GroupLifecycle::new());
        let permit = lifecycle.acquire(GroupId(4)).await.unwrap();
        let shutdown_calls = Arc::new(AtomicUsize::new(0));

        let task = {
            let lifecycle = Arc::clone(&lifecycle);
            let shutdown_calls = Arc::clone(&shutdown_calls);
            tokio::spawn(async move {
                lifecycle
                    .shutdown_with(async move {
                        shutdown_calls.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            })
        };

        tokio::task::yield_now().await;
        assert!(!lifecycle.is_accepting());
        assert_eq!(shutdown_calls.load(Ordering::SeqCst), 0);
        drop(permit);
        task.await.unwrap().unwrap();
        assert_eq!(shutdown_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            lifecycle.acquire(GroupId(4)).await,
            Err(MultiRaftError::GroupUnavailable {
                group_id: GroupId(4)
            })
        ));

        lifecycle
            .shutdown_with(async { panic!("second shutdown future must not run") })
            .await
            .unwrap();
        assert_eq!(shutdown_calls.load(Ordering::SeqCst), 1);
    }
}
