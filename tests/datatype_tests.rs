use tiledb_sys::Datatype;

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
