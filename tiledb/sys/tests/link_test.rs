#[test]
fn link_test() {
    let mut major: i32 = 0;
    let mut minor: i32 = 0;
    let mut patch: i32 = 0;

    unsafe {
        tiledb_sys::tiledb_version(&mut major, &mut minor, &mut patch);
    }

    println!("TileDB Version: {}.{}.{}", major, minor, patch);
}
