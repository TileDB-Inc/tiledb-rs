use tiledb_common::array::CellValNum;
use tiledb_common::datatype::Datatype;
use tiledb_common::filter::FilterData;
use tiledb_common::{metadata_value_go, physical_type_go};
use tiledb_serde::array::attribute::{AttributeData, FillData};

use super::{Attribute, Builder};
use crate::error::Error as TileDBError;
use crate::{Context, Factory, Result as TileDBResult};

impl TryFrom<&Attribute> for AttributeData {
    type Error = TileDBError;

    fn try_from(attr: &Attribute) -> Result<Self, Self::Error> {
        let datatype = attr.datatype()?;
        let fill = physical_type_go!(datatype, DT, {
            let (fill_value, fill_value_nullability) =
                attr.fill_value_nullable::<&[DT]>()?;
            FillData {
                data: fill_value.to_vec().into(),
                nullability: Some(fill_value_nullability),
            }
        });

        Ok(AttributeData {
            name: attr.name()?,
            datatype,
            nullability: Some(attr.is_nullable()?),
            cell_val_num: Some(attr.cell_val_num()?),
            fill: Some(fill),
            filters: Vec::<FilterData>::try_from(&attr.filter_list()?)?,
        })
    }
}

impl TryFrom<Attribute> for AttributeData {
    type Error = TileDBError;

    fn try_from(attribute: Attribute) -> Result<Self, Self::Error> {
        Self::try_from(&attribute)
    }
}

impl Factory for AttributeData {
    type Item = Attribute;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        let mut b = Builder::new(context, &self.name, self.datatype)?
            .filter_list(self.filters.create(context)?)?;

        if let Some(n) = self.nullability {
            b = b.nullability(n)?;
        }
        if let Some(c) = self.cell_val_num {
            if !matches!((self.datatype, c), (Datatype::Any, CellValNum::Var)) {
                /* SC-46696 */
                b = b.cell_val_num(c)?;
            }
        }
        if let Some(ref fill) = self.fill {
            b = metadata_value_go!(fill.data, _DT, ref value, {
                if let Some(fill_nullability) = fill.nullability {
                    b.fill_value_nullability(value.as_slice(), fill_nullability)
                } else {
                    b.fill_value(value.as_slice())
                }
            })?;
        }

        Ok(b.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Factory};
    use util::assert_option_subset;
    use util::option::OptionSubset;

    /// Test what the default values filled in for `None` with attribute data are.
    /// Mostly because if we write code which does need the default, we're expecting
    /// to match core and need to be notified if something changes or we did something
    /// wrong.
    #[test]
    fn attribute_defaults() {
        let ctx = Context::new().expect("Error creating context instance.");

        {
            let spec = AttributeData {
                name: "xkcd".to_owned(),
                datatype: Datatype::UInt32,
                ..Default::default()
            };
            let attr = spec.create(&ctx).unwrap();
            assert_eq!(CellValNum::single(), attr.cell_val_num().unwrap());

            // not nullable by default
            assert!(!attr.is_nullable().unwrap());
        }
        {
            let spec = AttributeData {
                name: "xkcd".to_owned(),
                datatype: Datatype::StringAscii,
                ..Default::default()
            };
            let attr = spec.create(&ctx).unwrap();
            assert_eq!(CellValNum::single(), attr.cell_val_num().unwrap());

            // not nullable by default
            assert!(!attr.is_nullable().unwrap());
        }
    }

    /// Test that the arbitrary attribute construction always succeeds
    #[test]
    fn attribute_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(attr in prop_attribute(Default::default()))| {
            attr.create(&ctx).expect("Error constructing arbitrary attribute");
        });
    }

    #[test]
    fn attribute_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(attr in prop_attribute(Default::default()))| {
            assert_eq!(attr, attr);
            assert_option_subset!(attr, attr);

            let attr = attr.create(&ctx)
                .expect("Error constructing arbitrary attribute");
            assert_eq!(attr, attr);
        });
    }
}
