mod benchmark_cache_db;
mod benchmark_query;

use criterion::{criterion_group, criterion_main};

criterion_group!(
    benches_query_serialize,
    benchmark_query::serialize_null,
    benchmark_query::serialize_file,
    benchmark_query::serialize_simple_string,
    benchmark_query::serialize_bulk_string,
    benchmark_query::serialize_array,
);
criterion_group!(benches_cache_db, benchmark_cache_db::bench_safe, benchmark_cache_db::bench_ptr);
criterion_main!(benches_query_serialize, benches_cache_db);
