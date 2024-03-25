use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tiledb::context::Context as TileDBContext;
use tiledb::filter::{FilterData, FilterListBuilder};
use tiledb::{error::Error as TileDBError, Result as TileDBResult};

use crate::datatype::{arrow_type_physical, tiledb_type_physical};

#[derive(Deserialize, Serialize)]
pub struct FilterMetadata {
    filters: Vec<FilterData>,
}

impl FilterMetadata {
    pub fn new(
        filters: &tiledb::filter_list::FilterList,
    ) -> TileDBResult<Self> {
        Ok(FilterMetadata {
            filters: filters
                .to_vec()?
                .into_iter()
                .map(|f| f.filter_data())
                .collect::<TileDBResult<Vec<FilterData>>>()?,
        })
    }

    pub fn apply<'ctx>(
        &self,
        mut filters: FilterListBuilder<'ctx>,
    ) -> TileDBResult<FilterListBuilder<'ctx>> {
        for filter in self.filters.iter() {
            filters = filters.add_filter_data(filter.clone())?;
        }
        Ok(filters)
    }
}

/// Encapsulates fields of a TileDB attribute which are not part of an Arrow field
#[derive(Deserialize, Serialize)]
pub struct AttributeMetadata {
    cell_val_num: u32,
    fill_value: Vec<u8>,
    filters: FilterMetadata,
}

impl AttributeMetadata {
    pub fn new(attr: &tiledb::array::Attribute) -> TileDBResult<Self> {
        Ok(AttributeMetadata {
            cell_val_num: attr.cell_val_num()?,
            fill_value: vec![], /* TODO */
            filters: FilterMetadata::new(&attr.filter_list()?)?,
        })
    }

    pub fn apply<'ctx>(
        &self,
        builder: tiledb::array::AttributeBuilder<'ctx>,
    ) -> TileDBResult<tiledb::array::AttributeBuilder<'ctx>> {
        /* TODO: fill value */
        let fl = self
            .filters
            .apply(FilterListBuilder::new(builder.context())?)?
            .build();
        builder.cell_val_num(self.cell_val_num)?.filter_list(&fl)
    }
}

pub fn arrow_field(
    attr: &tiledb::array::Attribute,
) -> TileDBResult<Option<arrow_schema::Field>> {
    if let Some(arrow_dt) = arrow_type_physical(&attr.datatype()?) {
        let name = attr.name()?;
        let metadata =
            serde_json::ser::to_string(&AttributeMetadata::new(attr)?)
                .map_err(|e| {
                    TileDBError::from(format!(
                        "Error serializing metadata for attribute {}: {}",
                        name, e
                    ))
                })?;
        Ok(Some(
            arrow_schema::Field::new(name, arrow_dt, attr.is_nullable())
                .with_metadata(HashMap::<String, String>::from([(
                    String::from("tiledb"),
                    metadata,
                )])),
        ))
    } else {
        Ok(None)
    }
}

pub fn tiledb_attribute<'ctx>(
    context: &'ctx TileDBContext,
    field: &arrow_schema::Field,
) -> TileDBResult<Option<tiledb::array::AttributeBuilder<'ctx>>> {
    if let Some(tiledb_dt) = tiledb_type_physical(field.data_type()) {
        let attr = tiledb::array::AttributeBuilder::new(
            context,
            field.name(),
            tiledb_dt,
        )?
        .nullability(field.is_nullable())?;

        if let Some(tiledb_metadata) = field.metadata().get("tiledb") {
            match serde_json::from_str::<AttributeMetadata>(
                tiledb_metadata.as_ref(),
            ) {
                Ok(attr_metadata) => Ok(Some(attr_metadata.apply(attr)?)),
                Err(e) => Err(TileDBError::from(format!(
                    "Error deserializing metadata for attribute {}: {}",
                    field.name(),
                    e
                ))),
            }
        } else {
            Ok(Some(attr))
        }
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

        proptest!(|(tdb_in in tiledb_test::attribute::arbitrary(&c))| {
            let tdb_in = tdb_in.expect("Error constructing arbitrary tiledb attribute");
            if let Some(arrow_field) = arrow_field(&tdb_in).expect("Error reading tiledb attribute") {
                // convert back to TileDB attribute
                let tdb_out = tiledb_attribute(&c, &arrow_field)?.expect("Arrow attribute did not invert").build();
                assert_eq!(tdb_in, tdb_out);
            }
        });

        // TODO: go the other direction

        Ok(())
    }
}
