use tiledb_sys::Datatype;
use tiledb_sys::Filesystem;

#[test]
fn datatype_roundtrips() {
    for i in 0..256 {
        let maybe_dt = Datatype::from_u32(i);
        if maybe_dt.is_some() {
            let dt = maybe_dt.unwrap();
            let dt_str = dt.to_string().expect("Error creating string.");
            let str_dt = Datatype::from_string(&dt_str)
                .expect("Error round tripping datatype string.");
            assert_eq!(str_dt, dt);
        }
    }
}

#[test]
fn filesystem_roundtrips() {
    for i in 0..256 {
        let maybe_fs = Filesystem::from_u32(i);
        if maybe_fs.is_some() {
            let fs = maybe_fs.unwrap();
            let fs_str = fs.to_string().expect("Error creating string.");
            let str_fs = Filesystem::from_string(&fs_str)
                .expect("Error round tripping datatype string.");
            assert_eq!(str_fs, fs);
        }
    }
}
