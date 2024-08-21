use tiledb::array::{
    ArrayType, AttributeData, CellValNum, DimensionConstraints, DimensionData,
    DomainData, SchemaData,
};
use tiledb::{physical_type_go, Datatype};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Parameters {
    pub allow_var: bool,
}

impl Parameters {
    pub fn disallow_var(&self) -> Self {
        Parameters { allow_var: false }
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
        let try_single =
            tiledb::datatype::arrow::to_arrow(&dt, CellValNum::single())
                .is_exact();
        let try_var = params.allow_var
            && tiledb::datatype::arrow::to_arrow(&dt, CellValNum::Var)
                .is_exact();

        if try_single && dt.is_allowed_dimension_type_sparse() {
            let constraints = physical_type_go!(dt, DT, {
                DimensionConstraints::from((&[0 as DT, 100 as DT], None))
            });
            dims.push(DimensionData {
                name: format!("d_{}", dt),
                datatype: dt,
                cell_val_num: None,
                filters: None,
                constraints,
            });
        }
        if try_var && dt == Datatype::StringAscii {
            dims.push(DimensionData {
                name: format!("d_{}", dt),
                datatype: dt,
                cell_val_num: Some(CellValNum::Var),
                filters: None,
                constraints: DimensionConstraints::StringAscii,
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
        if try_single {
            attfunc("single", CellValNum::single());
        }
        if try_var {
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
