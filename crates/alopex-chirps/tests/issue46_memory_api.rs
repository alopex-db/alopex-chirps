use alopex_chirps::memory::{MemoryComponent, MemoryConfig, MemoryManager};

#[test]
fn memory_manager_reports_breakdown_and_supports_runtime_budget_controls() {
    let manager = MemoryManager::new(MemoryConfig {
        total_budget: 100,
        message_buffer_limit: 60,
        raft_log_cache_limit: 20,
        connection_pool_limit: 10,
        ..MemoryConfig::default()
    })
    .expect("valid memory configuration");

    manager.set_component_usage(MemoryComponent::MessageBuffer, 70);
    manager.set_component_usage(MemoryComponent::RaftLogCache, 20);
    let stats = manager.get_memory_stats();
    assert_eq!(stats.total_budget, 100);
    assert_eq!(stats.message_buffer_bytes, 70);
    assert_eq!(stats.raft_log_cache_bytes, 20);
    assert_eq!(stats.current_usage, 90);
    assert!(!stats.budget_exceeded);

    manager.resize_memory_budget(80).unwrap();
    assert!(manager.get_memory_stats().budget_exceeded);

    manager.trigger_gc().unwrap();
    assert!(manager.get_memory_stats().current_usage <= 90);
}
