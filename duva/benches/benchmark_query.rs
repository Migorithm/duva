use bytes::Bytes;
use criterion::{Criterion, black_box};
use duva::domains::query_io::QueryIO;
use std::iter::repeat_n;

pub fn serialize_null(c: &mut Criterion) {
    c.bench_function("Bench Null Serialize", |b| {
        b.iter(|| {
            black_box(QueryIO::Null).serialize();
        })
    });
}

pub fn serialize_file(c: &mut Criterion) {
    c.bench_function("Bench File(value is 0) Serialize", |b| {
        b.iter(|| {
            black_box(QueryIO::File(Bytes::from_iter(repeat_n(0, 1024)))).serialize();
        })
    });
    c.bench_function("Bench File(value is 64) Serialize", |b| {
        b.iter(|| {
            black_box(QueryIO::File(Bytes::from_iter(repeat_n(64, 1024)))).serialize();
        })
    });
}

pub fn serialize_simple_string(c: &mut Criterion) {
    c.bench_function("Bench SimpleString(value is test) Serialize", |b| {
        b.iter(|| {
            black_box(QueryIO::SimpleString(repeat_n("test", 1024).collect())).serialize();
        })
    });
    c.bench_function("Bench SimpleString(value is 0000) Serialize", |b| {
        b.iter(|| {
            black_box(QueryIO::SimpleString(repeat_n("0000", 1024).collect())).serialize();
        })
    });
}

pub fn serialize_bulk_string(c: &mut Criterion) {
    c.bench_function("Bench BulkString(value is test) Serialize", |b| {
        b.iter(|| {
            black_box(QueryIO::BulkString(repeat_n("test", 1024).collect())).serialize();
        })
    });
    c.bench_function("Bench BulkString(value is 0000) Serialize", |b| {
        b.iter(|| {
            black_box(QueryIO::BulkString(repeat_n("0000", 1024).collect())).serialize();
        })
    });
}

pub fn serialize_array(c: &mut Criterion) {
    c.bench_function("Bench Array Serialize", |b| {
        b.iter(|| {
            black_box(QueryIO::Array(
                [
                    QueryIO::SimpleString(repeat_n("test", 1024).collect()),
                    QueryIO::BulkString(repeat_n("test", 1024).collect()),
                    QueryIO::Null,
                    QueryIO::Null,
                    QueryIO::File(Bytes::from_iter(repeat_n(64, 1024))),
                ]
                .into(),
            ))
            .serialize();
        })
    });
}
