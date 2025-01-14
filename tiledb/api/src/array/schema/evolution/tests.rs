use super::*;

use tiledb_common::array::{ArrayType, CellValNum};
use tiledb_common::Datatype;
use tiledb_pod::array::AttributeData;
use tiledb_utils::assert_option_subset;

use crate::array::Array;
use crate::tests::examples::{quickstart, TestArray};
use crate::Factory;

#[test]
fn add_attribute() -> anyhow::Result<()> {
    let array = TestArray::new(
        "add_attribute",
        quickstart::Builder::new(ArrayType::Sparse).build().into(),
    )?;

    let old_schema = array.for_read()?.schema()?;
    assert_eq!(1, old_schema.num_attributes()?);

    let new_attribute = AttributeData {
        name: "foobar".to_owned(),
        datatype: Datatype::Blob,
        cell_val_num: Some(CellValNum::Var),
        nullability: Some(false),
        fill: None,
        filters: Default::default(),
        enumeration: None,
    };

    let evolution = Builder::new(&array.context)?
        .add_attribute(new_attribute.create(&array.context)?)?
        .build();

    Array::evolve(&array.context, &array.uri, evolution)?;

    let new_schema = array.for_read()?.schema()?;
    assert_eq!(2, new_schema.num_attributes()?);

    assert_option_subset!(
        new_attribute,
        AttributeData::try_from(new_schema.attribute(1)?)?
    );

    Ok(())
}

#[test]
fn drop_attribute() -> anyhow::Result<()> {
    let array = TestArray::new("drop_attribute", {
        let mut b = quickstart::Builder::new(ArrayType::Sparse);
        b.schema.attributes.push(AttributeData {
            name: "foobar".to_owned(),
            datatype: Datatype::Blob,
            cell_val_num: Some(CellValNum::Var),
            nullability: Some(false),
            fill: None,
            filters: Default::default(),
            enumeration: None,
        });
        b.build().into()
    })?;

    let old_schema = array.for_read()?.schema()?;
    assert_eq!(2, old_schema.num_attributes()?);

    let evolution = Builder::new(&array.context)?.drop_attribute("a")?.build();

    Array::evolve(&array.context, &array.uri, evolution)?;

    let new_schema = array.for_read()?.schema()?;
    assert_eq!(1, new_schema.num_attributes()?);

    Ok(())
}
