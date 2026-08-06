use alopex_chirps::LocalHlc;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::time::Duration;

fn bench_local_hlc_tick(criterion: &mut Criterion) {
    let mut hlc = LocalHlc::new(Duration::from_secs(1));
    criterion.bench_function("local_hlc_tick", |bencher| {
        bencher.iter(|| black_box(hlc.tick()));
    });
}

criterion_group!(benches, bench_local_hlc_tick);
criterion_main!(benches);
