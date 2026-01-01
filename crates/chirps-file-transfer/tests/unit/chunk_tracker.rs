use alopex_chirps_file_transfer::ChunkTracker;

#[test]
fn chunk_tracker_tracks_state_and_retries() {
    let mut tracker = ChunkTracker::new(3, 2);

    let next = tracker.next_chunks(2);
    assert_eq!(next, vec![0, 1]);

    tracker.mark_in_flight(0);
    tracker.mark_in_flight(1);
    let next = tracker.next_chunks(2);
    assert_eq!(next, vec![2]);

    tracker.mark_failed(1);
    tracker.mark_completed(0);
    let next = tracker.next_chunks(2);
    assert_eq!(next, vec![1, 2]);

    tracker.mark_failed(1);
    assert_eq!(tracker.permanently_failed(), vec![1]);

    let next = tracker.next_chunks(2);
    assert_eq!(next, vec![2]);
}
