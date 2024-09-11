//! Provides methods for creating the "quickstart" example schema.

use crate::tests::prelude::*;

pub struct Builder {
    schema: SchemaData,
}

impl Builder {
    pub fn new(array_type: ArrayType) -> Self {
        Builder {
            schema: SchemaData {
                array_type,
                domain: DomainData {
                    dimension: vec![
                        DimensionData {
                            name: "rows".to_owned(),
                            datatype: Datatype::Int32,
                            constraints: DimensionConstraints::Int32(
                                [1, 4],
                                Some(4),
                            ),
                            filters: None,
                        },
                        DimensionData {
                            name: "cols".to_owned(),
                            datatype: Datatype::Int32,
                            constraints: DimensionConstraints::Int32(
                                [1, 4],
                                Some(4),
                            ),
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
            },
        }
    }

    pub fn with_rows(mut self, domain: DimensionConstraints) -> Self {
        let rows = &mut self.schema.domain.dimension[0];
        rows.datatype = domain.physical_datatype();
        rows.constraints = domain;
        self
    }

    pub fn with_cols(mut self, domain: DimensionConstraints) -> Self {
        let cols = &mut self.schema.domain.dimension[1];
        cols.datatype = domain.physical_datatype();
        cols.constraints = domain;
        self
    }

    pub fn attribute(&mut self) -> &mut AttributeData {
        &mut self.schema.attributes[0]
    }

    pub fn build(self) -> SchemaData {
        self.schema
    }
}
