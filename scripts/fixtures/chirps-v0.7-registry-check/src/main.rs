fn main() {
    let _ = alopex_chirps::NodeId::new();
    let _ = std::any::TypeId::of::<alopex_chirps_mock::MockBackend>();
}
