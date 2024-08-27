pub mod examples;

pub mod prelude {
    pub mod array {
        pub use crate::array::attribute::Builder as AttributeBuilder;
        pub use crate::array::dimension::Builder as DimensionBuilder;
        pub use crate::array::domain::Builder as DomainBuilder;
        pub use crate::array::schema::Builder as SchemaBuilder;
        pub use crate::array::{
            Array, ArrayType, Attribute, CellValNum, Dimension, Domain, Mode,
            Schema,
        };
    }

    pub mod query {
        pub use crate::query::{
            Query, QueryLayout, ReadBuilder, ReadQuery, WriteBuilder,
            WriteQuery,
        };
    }
}
