use tiledb::context::Context as TileDBContext;
use tiledb::Result as TileDBResult;

use crate::datatype::{arrow_type_physical, tiledb_type_physical};

pub fn arrow_field(
    attr: &tiledb::array::Attribute,
) -> TileDBResult<Option<arrow_schema::Field>> {
    if let Some(arrow_dt) = arrow_type_physical(&attr.datatype()?) {
        Ok(Some(arrow_schema::Field::new(
            attr.name()?,
            arrow_dt,
            attr.is_nullable(),
        )))
    } else {
        Ok(None)
    }
}

pub fn tiledb_attribute<'ctx>(
    context: &'ctx TileDBContext,
    field: &arrow_schema::Field,
) -> TileDBResult<Option<tiledb::array::AttributeBuilder<'ctx>>> {
    if let Some(tiledb_dt) = tiledb_type_physical(field.data_type()) {
        Ok(Some(
            tiledb::array::AttributeBuilder::new(
                context,
                field.name(),
                tiledb_dt,
            )?
            .nullability(field.is_nullable())?,
        ))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_invertibility() -> TileDBResult<()> {
        let c: TileDBContext = TileDBContext::new()?;

        proptest!(|(attr in tiledb_test::attribute::arbitrary(&c))| {
            if let Some(arrow_field) = arrow_field(&attr).expect("Error reading tiledb attribute") {
                assert_eq!(attr.name()?, *arrow_field.name());
                assert!(crate::datatype::is_same_physical_type(&attr.datatype()?, arrow_field.data_type()));
                assert_eq!(attr.is_nullable(), arrow_field.is_nullable());

                // convert back to TileDB attribute
                let tdb_out = tiledb_attribute(&c, &arrow_field)?.expect("Arrow attribute did not invert").build();
                assert_eq!(attr.name()?, tdb_out.name()?);
                assert_eq!(attr.datatype()?, tdb_out.datatype()?);
                assert_eq!(attr.is_nullable(), tdb_out.is_nullable());
            }
        });

        // TODO: go the other direction

        Ok(())
    }
}
