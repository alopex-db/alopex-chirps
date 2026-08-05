use super::{GroupHandle, GroupId, MultiRaftError, RaftStorageFactory};
use crate::raft::{ChirpsRaftTransport, RaftConfig, RaftNode};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

/// Owns the lifecycle registry for all Raft groups on one Chirps node.
pub struct MultiRaftManager<F> {
    transport: Arc<ChirpsRaftTransport>,
    factory: Arc<F>,
    raft_config: RaftConfig,
    lifecycle: Mutex<()>,
    groups: RwLock<HashMap<GroupId, Arc<GroupHandle>>>,
}

impl<F> MultiRaftManager<F>
where
    F: RaftStorageFactory,
{
    pub fn new(
        transport: Arc<ChirpsRaftTransport>,
        factory: Arc<F>,
        mut raft_config: RaftConfig,
    ) -> Self {
        raft_config.node_id = transport.node_id();
        Self {
            transport,
            factory,
            raft_config,
            lifecycle: Mutex::new(()),
            groups: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create_group(
        &self,
        group_id: GroupId,
        initial_members: BTreeSet<u64>,
        state_machine: F::StateMachine,
    ) -> Result<(), MultiRaftError> {
        let _lifecycle = self.lifecycle.lock().await;
        if self.groups_read().contains_key(&group_id) {
            return Err(MultiRaftError::GroupAlreadyExists { group_id });
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

        if let Err(error) = node.initialize(initial_members).await {
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

        let handle = Arc::new(GroupHandle::new(group_id, Arc::clone(&node)));
        let replaced = self.groups_write().insert(group_id, handle);
        if let Some(previous) = replaced {
            let inserted = self
                .groups_write()
                .insert(group_id, previous)
                .expect("newly inserted group must still be present");
            let _ = node.shutdown().await;
            drop(inserted);
            drop(node);
            let cleanup = transaction.abort().await;
            return match cleanup {
                Ok(()) => Err(MultiRaftError::GroupAlreadyExists { group_id }),
                Err(error) => Err(error),
            };
        }
        drop(transaction.commit());
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
        Ok(true)
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
