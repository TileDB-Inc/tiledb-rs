//! Provides methods for creating the "quickstart" example schema.

use crate::tests::prelude::*;

pub struct Builder {
    array_type: ArrayType,
    rows: DimensionConstraints,
    cols: DimensionConstraints,
}

impl Builder {
    pub fn new(array_type: ArrayType) -> Self {
        Builder {
            array_type,
            rows: DimensionConstraints::Int32([1, 4], Some(4)),
            cols: DimensionConstraints::Int32([1, 4], Some(4)),
        }
    }

    pub fn with_rows(mut self, domain: DimensionConstraints) -> Self {
        self.rows = domain;
        self
    }

    pub fn with_cols(mut self, domain: DimensionConstraints) -> Self {
        self.cols = domain;
        self
    }

    pub fn build(self) -> SchemaData {
        SchemaData {
            array_type: self.array_type,
            domain: DomainData {
                dimension: vec![
                    DimensionData {
                        name: "rows".to_owned(),
                        datatype: self.rows.physical_datatype(),
                        constraints: self.rows,
                        filters: None,
                    },
                    DimensionData {
                        name: "cols".to_owned(),
                        datatype: self.cols.physical_datatype(),
                        constraints: self.cols,
                        filters: None,
                    },
                ],
            },
            attributes: vec![AttributeData {
                name: "a".to_owned(),
                datatype: Datatype::Int32,
                ..Default::default()
            }],
            tile_order: Some(TileOrder::RowMajor),
            cell_order: Some(CellOrder::RowMajor),

            ..Default::default()
        }
    }
}
