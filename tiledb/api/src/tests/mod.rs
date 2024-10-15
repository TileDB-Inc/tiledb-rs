pub mod examples;

pub mod prelude {
    pub use tiledb_common::array::dimension::DimensionConstraints;
    pub use tiledb_common::array::{
        ArrayType, CellOrder, CellValNum, Mode, TileOrder,
    };
    pub use tiledb_common::datatype::Datatype;
    pub use tiledb_serde::array::attribute::AttributeData;
    pub use tiledb_serde::array::dimension::DimensionData;
    pub use tiledb_serde::array::domain::DomainData;
    pub use tiledb_serde::array::schema::SchemaData;

    pub use crate::array::attribute::Builder as AttributeBuilder;
    pub use crate::array::dimension::Builder as DimensionBuilder;
    pub use crate::array::domain::Builder as DomainBuilder;
    pub use crate::array::schema::Builder as SchemaBuilder;
    pub use crate::array::{Array, Attribute, Dimension, Domain, Schema};

    pub use crate::query::{
        Query, QueryBuilder, QueryLayout, ReadBuilder, ReadQuery, WriteBuilder,
        WriteQuery,
    };

    pub use super::examples::TestArray;
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy {
    pub mod prelude {
        // NB: this is hardly exhaustive, feel free to add stuff, this is just what has been needed
        // so far

        pub use cells::write::strategy::{
            DenseWriteParameters, SparseWriteParameters,
        };
        pub use cells::write::{DenseWriteInput, SparseWriteInput, WriteInput};
        pub use cells::{Cells, FieldData};
    }
}
