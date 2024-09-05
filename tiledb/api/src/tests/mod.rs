pub mod examples;

pub mod prelude {
    pub use crate::array::attribute::{
        AttributeData, Builder as AttributeBuilder,
    };
    pub use crate::array::dimension::{
        Builder as DimensionBuilder, DimensionConstraints, DimensionData,
    };
    pub use crate::array::domain::{Builder as DomainBuilder, DomainData};
    pub use crate::array::schema::{Builder as SchemaBuilder, SchemaData};
    pub use crate::array::{
        Array, ArrayType, Attribute, CellOrder, CellValNum, Dimension, Domain,
        Mode, Schema, TileOrder,
    };
    pub use crate::Datatype;

    pub use crate::query::{
        Query, QueryBuilder, QueryLayout, ReadBuilder, ReadQuery, WriteBuilder,
        WriteQuery,
    };
}

pub mod strategy {
    pub mod prelude {
        // NB: this is hardly exhaustive, feel free to add stuff, this is just what has been needed
        // so far

        pub use crate::query::strategy::{Cells, FieldData};
        pub use crate::query::write::strategy::{
            DenseWriteInput, DenseWriteParameters, SparseWriteInput,
            SparseWriteParameters, WriteInput,
        };
    }
}
