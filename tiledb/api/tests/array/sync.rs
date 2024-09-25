extern crate tiledb;

use tiledb::array::Array;

/// Fails to compile unless `T` is `Sync`.
fn require_sync<T>()
where
    T: Sync,
{
}

/// Check whether `Array` is `Sync`.
///
/// This is expected to fail because `Array` must not be `Sync`
/// for the query API to be thread-safe.
///
/// In core it is not safe to submit multiple queries concurrently
/// against the same open array. The Rust API therefore must
/// prevent multiple concurrent calls to `tiledb_query_submit`
/// occurring concurrently for the same `Array` instance.
///
/// This can be preventing using `&mut Array` in the query API,
/// and while that feels right for write queries, it does not
/// feel right for read queries. To enable `&Array` for read
/// queries, instead we must require:
/// 1) we do not support async query submit
/// 2) we cannot share an `Array` between multiple threads
///
/// This test will intentionally fail to compile as long as (2) is true,
fn main() {
    require_sync::<Array>();
}
