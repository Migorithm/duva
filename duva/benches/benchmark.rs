use criterion::{Criterion, criterion_group, criterion_main};

fn bench_test(c: &mut Criterion) {
    c.bench_function("Test Bench", |b| b.iter(|| ()));
}

criterion_group!(benches, bench_test);
criterion_main!(benches);
