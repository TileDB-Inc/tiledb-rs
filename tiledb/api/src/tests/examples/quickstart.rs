//! Provides methods for creating the "quickstart" example schema.

use crate::tests::prelude::*;

pub struct Builder {
    pub schema: SchemaData,
}

impl Builder {
    pub fn new(array_type: ArrayType) -> Self {
        let schema = SchemaData::new(
            array_type,
            vec![
                DimensionData::new("rows", 1, 4, Some(4)),
                DimensionData::new("cols", 1, 4, Some(4)),
            ],
            vec![AttributeData::new("a", Datatype::Int32)],
        )
        .with_tile_order(TileOrder::RowMajor)
        .with_cell_order(CellOrder::RowMajor);
        Builder { schema }
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
