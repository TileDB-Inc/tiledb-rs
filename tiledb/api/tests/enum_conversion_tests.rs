use tiledb::Datatype;
use tiledb_sys::Filesystem;
use tiledb_sys::FilterOption;
use tiledb_sys::FilterType;

#[test]
fn datatype_roundtrips() {
    for i in 0..256 {
        let maybe_dt = Datatype::try_from(i);
        if maybe_dt.is_ok() {
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
                .expect("Error round tripping filesystem string.");
            assert_eq!(str_fs, fs);
        }
    }
}

#[test]
fn filter_option_roundtrips() {
    for i in 0..256 {
        let maybe_fopt = FilterOption::from_u32(i);
        if maybe_fopt.is_some() {
            let fopt = maybe_fopt.unwrap();
            let fopt_str = fopt.to_string().expect("Error creating string.");
            let str_fopt = FilterOption::from_string(&fopt_str)
                .expect("Error round tripping filter option string.");
            assert_eq!(str_fopt, fopt);
        }
    }
}

#[test]
fn filter_type_roundtrips() {
    for i in 0..256 {
        let maybe_ftype = FilterType::from_u32(i);
        if maybe_ftype.is_some() {
            let ftype = maybe_ftype.unwrap();
            let ftype_str = ftype.to_string().expect("Error creating string.");
            let str_ftype = FilterType::from_string(&ftype_str)
                .expect("Error round tripping filter type string.");
            assert_eq!(str_ftype, ftype);
        }
    }
}
