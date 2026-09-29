use alopex_chirps::{
    AllocationRatio, IntegratedCacheManager, MemoryConfig, MessageProfile, WorkloadProfile,
};

#[test]
fn integrated_cache_rebalances_and_evicts_against_one_budget() {
    let config = MemoryConfig {
        total_budget: 1_000,
        message_buffer_limit: 500,
        raft_log_cache_limit: 400,
        connection_pool_limit: 300,
        ..MemoryConfig::default()
    };
    let mut manager = IntegratedCacheManager::new(config).expect("integrated cache");
    manager
        .message_buffer
        .push(MessageProfile::Ephemeral, vec![0; 250])
        .expect("message buffer capacity");
    manager.raft_cache.insert(1, vec![0; 250]);
    manager.block_cache.set_used_bytes(500);

    manager.rebalance(WorkloadProfile {
        message_buffer: 0.20,
        raft_cache: 0.20,
        connection_pool: 0.10,
        block_cache: 0.50,
    });
    assert_eq!(
        manager.allocation_ratio,
        AllocationRatio {
            message_buffer: 0.20,
            raft_cache: 0.20,
            connection_pool: 0.10,
            block_cache: 0.50,
        }
    );

    manager.emergency_evict(400);
    let metrics = manager.get_unified_metrics();
    assert!(metrics.current_usage <= metrics.total_budget);
    assert!(metrics.evicted_bytes >= 400);
}
