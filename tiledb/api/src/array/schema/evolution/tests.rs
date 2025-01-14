use super::*;

use tiledb_common::array::{ArrayType, CellValNum};
use tiledb_common::Datatype;
use tiledb_pod::array::{AttributeData, EnumerationData};
use tiledb_utils::assert_option_subset;

use crate::array::schema::EnumerationKey;
use crate::array::Array;
use crate::tests::examples::{quickstart, TestArray};
use crate::Factory;

/// Test adding an attribute
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

/// Test dropping an attribute
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

/// Test adding an enumeration (and an attribute which uses it)
#[test]
fn add_enumeration() -> anyhow::Result<()> {
    let array = TestArray::new(
        "add_enumeration",
        quickstart::Builder::new(ArrayType::Sparse).build().into(),
    )?;

    let old_schema = array.for_read()?.schema()?;
    assert_eq!(1, old_schema.num_attributes()?);

    let ename = "states_enumeration";

    let new_attribute = AttributeData {
        name: "state".to_owned(),
        datatype: Datatype::UInt8,
        cell_val_num: None,
        nullability: Some(false),
        fill: None,
        filters: Default::default(),
        enumeration: Some(ename.to_owned()),
    };

    let states_enumeration = EnumerationData {
        name: ename.to_owned(),
        datatype: Datatype::StringAscii,
        cell_val_num: Some(CellValNum::Var),
        ordered: None,
        data: "newhampshirenewjerseynewyork"
            .as_bytes()
            .to_vec()
            .into_boxed_slice(),
        offsets: Some(vec![0, 12, 21].into_boxed_slice()),
    };

    let evolution = Builder::new(&array.context)?
        .add_attribute(new_attribute.create(&array.context)?)?
        .add_enumeration(states_enumeration.create(&array.context)?)?
        .build();

    Array::evolve(&array.context, &array.uri, evolution)?;

    let new_schema = array.for_read()?.schema()?;
    assert_eq!(2, new_schema.num_attributes()?);

    let added_attribute = AttributeData::try_from(new_schema.attribute(1)?)?;

    assert_option_subset!(new_attribute, added_attribute);
    assert_eq!(Some(ename), added_attribute.enumeration.as_deref());

    let added_enumeration = EnumerationData::try_from(
        new_schema.enumeration(EnumerationKey::EnumerationName(ename))?,
    )?;
    assert_option_subset!(states_enumeration, added_enumeration);

    Ok(())
}
