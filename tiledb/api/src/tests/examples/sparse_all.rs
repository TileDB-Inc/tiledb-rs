use crate::array::{
    ArrayType, AttributeData, CellValNum, DimensionConstraints, DimensionData,
    DomainData, SchemaData,
};
use crate::{physical_type_go, Datatype};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Parameters {
    // TODO: this probably oughta be a function object to accept/reject datatype/CVN pairs
    pub allow_var: bool,
}

impl Parameters {
    fn try_dimension(&self, dt: Datatype) -> bool {
        if dt == Datatype::StringAscii {
            // FIXME: "Offsets buffer is not set" for some reason
            false
        } else {
            dt.is_allowed_dimension_type_sparse()
        }
    }

    fn try_attribute(&self, dt: Datatype, cell_val_num: CellValNum) -> bool {
        let is_supported_datatype = !matches!(dt, Datatype::StringUcs2);

        if !is_supported_datatype {
            false
        } else if cell_val_num.is_var_sized() {
            self.allow_var
        } else {
            true
        }
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Parameters { allow_var: true }
    }
}

/// Returns a schema which contains a dimension of all allowed types for a sparse array schema,
/// and one attribute of all datatype/nullability combinations for `CellValNum::single()` and
/// `CellValNum::Var`, depending on settings from `params`.
pub fn schema(params: Parameters) -> SchemaData {
    // build a schema with one dimension/attribute of all possible types
    let mut dims = vec![];
    let mut atts = vec![];
    for dt in Datatype::iter() {
        if params.try_dimension(dt) {
            let constraints = if dt != Datatype::StringAscii {
                physical_type_go!(dt, DT, {
                    DimensionConstraints::from((&[0 as DT, 100 as DT], None))
                })
            } else {
                DimensionConstraints::StringAscii
            };
            dims.push(DimensionData {
                name: format!("d_{}", dt),
                datatype: dt,
                cell_val_num: None,
                filters: None,
                constraints,
            });
        }

        let mut attfunc = |tag, cell_val_num| {
            atts.push(AttributeData {
                name: format!("a_{}_{}", dt, tag),
                datatype: dt,
                nullability: Some(true),
                cell_val_num: Some(cell_val_num),
                fill: None,
                filters: Default::default(),
            });
            atts.push(AttributeData {
                name: format!("a_{}_{}_not_nullable", dt, tag),
                datatype: dt,
                nullability: Some(false),
                cell_val_num: Some(cell_val_num),
                fill: None,
                filters: Default::default(),
            });
        };

        if params.try_attribute(dt, CellValNum::single()) {
            attfunc("single", CellValNum::single());
        }
        if params.try_attribute(dt, CellValNum::Var) {
            attfunc("var", CellValNum::Var);
        }
    }

    SchemaData {
        array_type: ArrayType::Sparse,
        domain: DomainData { dimension: dims },
        capacity: None,
        cell_order: None,
        tile_order: None,
        allow_duplicates: None,
        attributes: atts,
        coordinate_filters: Default::default(),
        offsets_filters: Default::default(),
        nullity_filters: Default::default(),
    }
}
