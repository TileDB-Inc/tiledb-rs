use proptest::test_runner::TestRng;

use rand::Rng;

use tiledb::array::attribute::AttributeData;
use tiledb::array::dimension::DimensionData;
use tiledb::array::schema::CellValNum;
use tiledb::array::schema::SchemaData;
use tiledb::datatype::Datatype;
use tiledb::error::Error;
use tiledb::filter::list::FilterListData;
use tiledb::Result as TileDBResult;

use crate::filter;

#[derive(Copy, Clone)]
pub enum FilterListContextKind {
    Attribute,
    Dimension,
    Coordinates,
    Offsets,
    Nullity,
}

pub fn generate(
    rng: &mut TestRng,
    kind: FilterListContextKind,
    schema: &SchemaData,
    datatype: Datatype,
    cell_val_num: CellValNum,
) -> TileDBResult<FilterListData> {
    let num_filters = rng.gen_range(0..=8);
    let mut filters = Vec::new();
    let mut curr_type = datatype;
    for idx in 0..num_filters {
        let fdata = filter::generate_with_constraints(
            rng,
            kind,
            schema,
            datatype,
            curr_type,
            cell_val_num,
            idx,
        );
        let next_type = fdata.transform_datatype(&curr_type);
        if next_type.is_none() {
            return Err(Error::Other(format!(
                "INVALID FILTER DATA: {} {} {:?}",
                curr_type, cell_val_num, fdata
            )));
        }
        filters.push(fdata);
        curr_type = next_type.unwrap();
    }

    Ok(FilterListData::from_iter(filters))
}

pub fn gen_for_dimension(
    rng: &mut TestRng,
    schema: &SchemaData,
    dim: &DimensionData,
) -> TileDBResult<FilterListData> {
    let cell_val_num = if matches!(dim.datatype, Datatype::StringAscii) {
        CellValNum::Var
    } else {
        CellValNum::single()
    };
    generate(
        rng,
        FilterListContextKind::Dimension,
        schema,
        dim.datatype,
        cell_val_num,
    )
}

pub fn gen_for_attribute(
    rng: &mut TestRng,
    schema: &SchemaData,
    attr: &AttributeData,
) -> TileDBResult<FilterListData> {
    let cell_val_num = if matches!(attr.datatype, Datatype::Any) {
        CellValNum::Var
    } else {
        attr.cell_val_num.unwrap()
    };
    generate(
        rng,
        FilterListContextKind::Attribute,
        schema,
        attr.datatype,
        cell_val_num,
    )
}
