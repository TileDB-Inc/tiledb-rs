//! Provides methods for creating a schema which can optionally have
//! one dimension of each allowed datatype and one attribute of each
//! allowed `(Datatype, CellValNum, Nullability)` tuple.

use std::rc::Rc;

use tiledb_common::array::dimension::DimensionConstraints;
use tiledb_common::array::{ArrayType, CellValNum};
use tiledb_common::datatype::Datatype;
use tiledb_common::physical_type_go;
use tiledb_pod::array::attribute::AttributeData;
use tiledb_pod::array::dimension::DimensionData;
use tiledb_pod::array::domain::DomainData;
use tiledb_pod::array::schema::SchemaData;

pub type FnAcceptDimension = dyn Fn(&Parameters, Datatype) -> bool;
pub type FnAcceptAttribute =
    dyn Fn(&Parameters, Datatype, CellValNum, bool) -> bool;

/// Configures construction of the `sparse_all` schema.
#[derive(Clone)]
pub struct Parameters {
    /// Function which determines whether to add a dimension to the schema.
    ///
    /// By default, all types are added as dimensions except `Datatype::StringAscii`.
    pub fn_accept_dimension: Rc<FnAcceptDimension>,

    /// Function which determines whether to add an attribute to the schema.
    ///
    /// By default, all attributes are accepted.
    pub fn_accept_attribute: Rc<FnAcceptAttribute>,
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

    fn try_attribute(
        &self,
        _dt: Datatype,
        _cell_val_num: CellValNum,
        _is_nullable: bool,
    ) -> bool {
        true
    }

    fn default_accept_dimension(params: &Self, dt: Datatype) -> bool {
        params.try_dimension(dt)
    }

    fn default_accept_attribute(
        params: &Self,
        dt: Datatype,
        cell_val_num: CellValNum,
        is_nullable: bool,
    ) -> bool {
        params.try_attribute(dt, cell_val_num, is_nullable)
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Parameters {
            fn_accept_dimension: Rc::new(Self::default_accept_dimension),
            fn_accept_attribute: Rc::new(Self::default_accept_attribute),
        }
    }
}

/// Returns a sparse array schema which contains up to one dimension for each
/// allowed datatype and up to one attribute for each allowed
/// `Datatype`, `CellValNum`, and nullability.
pub fn schema(params: Parameters) -> SchemaData {
    // build a schema with one dimension/attribute of all possible types
    let mut dims = vec![];
    let mut atts = vec![];
    for dt in Datatype::iter() {
        if (params.fn_accept_dimension)(&params, dt) {
            let constraints = if dt != Datatype::StringAscii {
                physical_type_go!(dt, DT, {
                    DimensionConstraints::from((&[0 as DT, 100 as DT], None))
                })
            } else {
                DimensionConstraints::StringAscii
            };
            dims.push(DimensionData {
                name: format!("d_{dt}"),
                datatype: dt,
                filters: None,
                constraints,
            });
        }

        let mut attfunc = |cell_val_num, is_nullable| {
            let tag_cvn = match cell_val_num {
                CellValNum::Fixed(nz) if nz.get() == 1 => "single".to_owned(),
                CellValNum::Fixed(nz) => format!("fixed@{nz}"),
                CellValNum::Var => "var".to_owned(),
            };
            let tag_nullable = if is_nullable {
                "nullable"
            } else {
                "not_nullable"
            };

            atts.push(AttributeData {
                name: format!("a_{dt}_{tag_cvn}_{tag_nullable}"),
                datatype: dt,
                nullability: Some(is_nullable),
                cell_val_num: Some(cell_val_num),
                fill: None,
                filters: Default::default(),
                enumeration: None,
            });
        };

        if (params.fn_accept_attribute)(
            &params,
            dt,
            CellValNum::single(),
            false,
        ) {
            attfunc(CellValNum::single(), false);
        }
        if (params.fn_accept_attribute)(&params, dt, CellValNum::single(), true)
        {
            attfunc(CellValNum::single(), true);
        }
        if (params.fn_accept_attribute)(&params, dt, CellValNum::Var, false) {
            attfunc(CellValNum::Var, false);
        }
        if (params.fn_accept_attribute)(&params, dt, CellValNum::Var, true) {
            attfunc(CellValNum::Var, true);
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
        enumerations: Default::default(),
        coordinate_filters: Default::default(),
        offsets_filters: Default::default(),
        validity_filters: Default::default(),
    }
}
