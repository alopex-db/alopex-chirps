use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_mock::deterministic::{
    DeterministicNetwork, DirectedLink, EventKind, FaultEffect, FaultSchedule,
    FaultScheduleBuilder, minimize_schedule,
};
use alopex_chirps_wire::{frame::Frame, node_id::NodeId};

fn node(byte: u8) -> NodeId {
    NodeId::from([byte; 16])
}

fn ping(seq: u64, from: NodeId) -> Frame {
    Frame::Ping { seq, from }
}

async fn replay(seed: u64) -> anyhow::Result<(Vec<String>, Vec<u64>)> {
    let a = node(1);
    let b = node(2);
    let link = DirectedLink::new(a, b);
    let schedule = FaultSchedule::from_seed(seed, [link], 8);
    let network = DeterministicNetwork::new(schedule);
    let backend_a = network.add_node(a).await;
    let backend_b = network.add_node(b).await;
    let mut incoming = backend_b.subscribe().await?;

    for seq in 0..8 {
        backend_a.send(b, ping(seq, a)).await?;
    }
    network.run_until_idle().await?;

    let mut delivered = Vec::new();
    while let Ok((_source, Frame::Ping { seq, .. })) = incoming.try_recv() {
        delivered.push(seq);
    }
    Ok((network.stable_trace().await, delivered))
}

#[tokio::test]
async fn identical_seed_replays_identically() -> anyhow::Result<()> {
    let first = replay(0x603).await?;
    let second = replay(0x603).await?;
    assert_eq!(first, second);
    Ok(())
}

#[test]
fn seed_expansion_changes_and_round_trips() {
    let link = DirectedLink::new(node(1), node(2));
    let first = FaultSchedule::from_seed(0x603, [link], 16);
    let second = FaultSchedule::from_seed(0x604, [link], 16);
    assert_ne!(first, second);
    let encoded = serde_json::to_vec(&first).unwrap();
    let decoded: FaultSchedule = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, first);
}

#[tokio::test]
async fn deliver_next_resolves_one_packet_copy() -> anyhow::Result<()> {
    let a = node(1);
    let b = node(2);
    let network = DeterministicNetwork::new(FaultSchedule::empty(1));
    let backend_a = network.add_node(a).await;
    let backend_b = network.add_node(b).await;
    let mut incoming = backend_b.subscribe().await?;
    backend_a.send(b, ping(1, a)).await?;
    backend_a.send(b, ping(2, a)).await?;

    assert_eq!(network.pending_count().await, 2);
    assert!(network.deliver_next().await?);
    assert_eq!(network.pending_count().await, 1);
    assert!(matches!(
        incoming.try_recv(),
        Ok((_, Frame::Ping { seq: 1, .. }))
    ));
    assert!(incoming.try_recv().is_err());
    assert!(network.deliver_next().await?);
    assert!(!network.deliver_next().await?);
    Ok(())
}

#[tokio::test]
async fn directed_link_faults_are_composable() -> anyhow::Result<()> {
    let a = node(1);
    let b = node(2);
    let link = DirectedLink::new(a, b);
    let schedule = FaultScheduleBuilder::new(7)
        .on_nth(link, 0, [FaultEffect::Reorder { extra_ticks: 5 }])
        .on_nth(
            link,
            2,
            [
                FaultEffect::Delay { ticks: 3 },
                FaultEffect::Duplicate { copies: 2 },
            ],
        )
        .on_nth(link, 3, [FaultEffect::Drop])
        .build();
    let network = DeterministicNetwork::new(schedule);
    let backend_a = network.add_node(a).await;
    let backend_b = network.add_node(b).await;
    let mut incoming = backend_b.subscribe().await?;

    backend_a.send(b, ping(1, a)).await?;
    backend_a.send(b, ping(2, a)).await?;
    backend_a.send(b, ping(3, a)).await?;
    backend_a.send(b, ping(4, a)).await?;
    network.advance_by(2).await?;
    assert!(matches!(incoming.try_recv(), Ok((source, Frame::Ping { seq: 2, .. })) if source == a));
    network.run_until_idle().await?;

    let mut delivered = vec![2];
    while let Ok((_source, Frame::Ping { seq, .. })) = incoming.try_recv() {
        delivered.push(seq);
    }
    assert_eq!(delivered, vec![2, 3, 3, 1]);
    let events = network.events().await;
    assert!(events.iter().any(|event| event.kind == EventKind::Dropped));
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::Delivered)
            .count(),
        4
    );
    Ok(())
}

#[tokio::test]
async fn partition_heal_is_asymmetric() -> anyhow::Result<()> {
    let a = node(1);
    let b = node(2);
    let ab = DirectedLink::new(a, b);
    let ba = DirectedLink::new(b, a);
    let schedule = FaultScheduleBuilder::new(11)
        .on_nth(ab, 0, [FaultEffect::Delay { ticks: 3 }])
        .build();
    let network = DeterministicNetwork::new(schedule);
    let backend_a = network.add_node(a).await;
    let backend_b = network.add_node(b).await;
    let mut incoming_a = backend_a.subscribe().await?;
    let mut incoming_b = backend_b.subscribe().await?;

    backend_a.send(b, ping(1, a)).await?;
    network.partition(ab).await;
    backend_b.send(a, ping(2, b)).await?;
    network.advance_by(3).await?;
    network.run_until_idle().await?;
    assert!(incoming_b.try_recv().is_err());
    assert!(
        matches!(incoming_a.try_recv(), Ok((source, Frame::Ping { seq: 2, .. })) if source == b)
    );

    network.heal(ab).await;
    assert!(!network.is_partitioned(ab).await);
    assert!(!network.is_partitioned(ba).await);
    network.run_until_idle().await?;
    assert!(
        matches!(incoming_b.try_recv(), Ok((source, Frame::Ping { seq: 1, .. })) if source == a)
    );
    Ok(())
}

#[tokio::test]
async fn reconnect_discards_stale_generation() -> anyhow::Result<()> {
    let a = node(1);
    let b = node(2);
    let link = DirectedLink::new(a, b);
    let schedule = FaultScheduleBuilder::new(13)
        .on_nth(link, 0, [FaultEffect::Delay { ticks: 5 }])
        .build();
    let network = DeterministicNetwork::new(schedule);
    let backend_a = network.add_node(a).await;
    let old_b = network.add_node(b).await;
    let mut old_incoming = old_b.subscribe().await?;

    backend_a.send(b, ping(1, a)).await?;
    let new_b = network.add_node(b).await;
    let mut new_incoming = new_b.subscribe().await?;
    network.run_until_idle().await?;

    assert!(old_incoming.try_recv().is_err());
    assert!(new_incoming.try_recv().is_err());
    assert!(
        network
            .events()
            .await
            .iter()
            .any(|event| event.kind == EventKind::StaleGeneration)
    );
    Ok(())
}

#[tokio::test]
async fn reconnecting_source_discards_its_queued_generation() -> anyhow::Result<()> {
    let a = node(1);
    let b = node(2);
    let link = DirectedLink::new(a, b);
    let schedule = FaultScheduleBuilder::new(19)
        .on_nth(link, 0, [FaultEffect::Delay { ticks: 5 }])
        .build();
    let network = DeterministicNetwork::new(schedule);
    let old_a = network.add_node(a).await;
    let backend_b = network.add_node(b).await;
    let mut incoming_b = backend_b.subscribe().await?;

    old_a.send(b, ping(1, a)).await?;
    let _new_a = network.add_node(a).await;
    network.run_until_idle().await?;

    assert!(incoming_b.try_recv().is_err());
    assert!(
        network
            .events()
            .await
            .iter()
            .any(|event| event.kind == EventKind::StaleGeneration)
    );
    Ok(())
}

#[test]
fn failing_schedule_is_minimized_from_fresh_candidates() {
    let a = node(1);
    let b = node(2);
    let link = DirectedLink::new(a, b);
    let schedule = FaultScheduleBuilder::new(17)
        .on_nth(link, 0, [FaultEffect::Delay { ticks: 10 }])
        .on_nth(link, 1, [FaultEffect::Drop])
        .on_nth(link, 2, [FaultEffect::Duplicate { copies: 3 }])
        .build();

    let minimized = minimize_schedule(&schedule, |candidate| {
        candidate
            .rules()
            .iter()
            .any(|rule| rule.effects.contains(&FaultEffect::Drop))
    });
    assert_eq!(minimized.rules().len(), 1);
    assert_eq!(minimized.rules()[0].effects, vec![FaultEffect::Drop]);
}
