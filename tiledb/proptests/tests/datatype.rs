use std::collections::HashSet;

use proptest::prelude::*;

use tiledb::datatype::{Datatype, LogicalType};
use tiledb::error::Error;
use tiledb::{fn_typed, Result as TileDBResult};

use tiledb_proptests::datatype as pt_datatype;

fn check_capi_roundtrip(datatype: Datatype) -> TileDBResult<()> {
    let capi = datatype.capi_enum();
    let roundtrip = Datatype::try_from(capi)?;
    assert_eq!(roundtrip, datatype);
    Ok(())
}

fn check_string_roundtrip(datatype: Datatype) -> TileDBResult<()> {
    let dtstring = if let Some(dtstring) = datatype.to_string() {
        dtstring
    } else {
        return Err(Error::Other("Invalid datatype.".to_string()));
    };

    if let Some(roundtrip) = Datatype::from_string(&dtstring) {
        assert_eq!(roundtrip, datatype);
    } else {
        return Err(Error::Other("Invalid string.".to_string()));
    }

    Ok(())
}

fn check_fn_typed_type_and_size(datatype: Datatype) -> TileDBResult<()> {
    fn_typed!(datatype, LT, {
        type DT = <LT as LogicalType>::PhysicalType;
        assert!(datatype.is_compatible_type::<DT>());
        assert_eq!(datatype.size(), std::mem::size_of::<DT>() as u64);
    });

    Ok(())
}

fn check_dt_is_type_methods(datatype: Datatype) -> TileDBResult<()> {
    let num_types: u64 = [
        if datatype.is_integral_type() { 1 } else { 0 },
        if datatype.is_real_type() { 1 } else { 0 },
        if datatype.is_string_type() { 1 } else { 0 },
        if datatype.is_datetime_type() { 1 } else { 0 },
        if datatype.is_time_type() { 1 } else { 0 },
        if datatype.is_byte_type() { 1 } else { 0 },
    ]
    .iter()
    .sum();

    let expect_num_types = match datatype {
        Datatype::Any => 0,
        Datatype::Char => 0,
        _ => 1,
    };

    assert_eq!(num_types, expect_num_types);

    Ok(())
}

fn check_dt_is_dense_dimension(datatype: Datatype) -> TileDBResult<()> {
    let dense_types = pt_datatype::dense_dimension_datatypes_vec()
        .into_iter()
        .collect::<HashSet<_>>();

    assert_eq!(
        datatype.is_allowed_dimension_type_dense(),
        dense_types.contains(&datatype)
    );

    Ok(())
}

fn check_dt_is_sparse_dimension(datatype: Datatype) -> TileDBResult<()> {
    let sparse_types = pt_datatype::sparse_dimension_datatypes_vec()
        .into_iter()
        .collect::<HashSet<_>>();

    assert_eq!(
        datatype.is_allowed_dimension_type_sparse(),
        sparse_types.contains(&datatype)
    );

    Ok(())
}

#[test]
fn capi_enum_roundtrip() {
    let cfg = ProptestConfig::with_cases(1000);

    proptest!(cfg, |(dt in pt_datatype::prop_all_datatypes())| {
        check_capi_roundtrip(dt)?;
        check_string_roundtrip(dt)?;
        check_fn_typed_type_and_size(dt)?;
        check_dt_is_type_methods(dt)?;
        check_dt_is_dense_dimension(dt)?;
        check_dt_is_sparse_dimension(dt)?;
    })
}
