use std::collections::HashMap;
use std::sync::Arc;

use arrow::array as aa;
use arrow::datatypes as adt;
use tiledb_api::query::read::aggregate::AggregateFunction;

use super::QueryBuilder;

/// Default field capacity is 10MiB
const DEFAULT_CAPACITY: usize = 1024 * 1024 * 10;

#[derive(Debug, Default)]
pub enum QueryField {
    #[default]
    Default,
    WithCapacity(usize),
    WithCapacityAndType(usize, adt::DataType),
    WithType(adt::DataType),
    Buffer(Arc<dyn aa::Array>),
}

impl QueryField {
    pub fn capacity(&self) -> Option<usize> {
        match self {
            Self::Default => Some(DEFAULT_CAPACITY),
            Self::WithCapacity(capacity) => Some(*capacity),
            Self::WithCapacityAndType(capacity, _) => Some(*capacity),
            Self::WithType(_) => Some(DEFAULT_CAPACITY),
            Self::Buffer(_) => None,
        }
    }

    pub fn target_type(&self) -> Option<adt::DataType> {
        match self {
            Self::Default => None,
            Self::WithCapacity(_) => None,
            Self::WithCapacityAndType(_, dtype) => Some(dtype.clone()),
            Self::WithType(dtype) => Some(dtype.clone()),
            Self::Buffer(array) => Some(array.data_type().clone()),
        }
    }
}

#[derive(Debug, Default)]
pub struct QueryFields {
    pub fields: HashMap<String, QueryField>,
    pub aggregates: HashMap<String, (AggregateFunction, QueryField)>,
}

impl QueryFields {
    pub fn insert<S: Into<String>>(&mut self, name: S, field: QueryField) {
        let name: String = name.into();
        self.fields.insert(name.clone(), field);
    }
}

#[derive(Default)]
pub struct QueryFieldsBuilder {
    fields: QueryFields,
}

impl QueryFieldsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(self) -> QueryFields {
        self.fields
    }

    pub fn field(mut self, name: &str) -> Self {
        self.fields.insert(name, Default::default());
        self
    }

    pub fn field_with_buffer(
        mut self,
        name: &str,
        buffer: Arc<dyn aa::Array>,
    ) -> Self {
        self.fields.insert(name, QueryField::Buffer(buffer));
        self
    }

    pub fn field_with_capacity(mut self, name: &str, capacity: usize) -> Self {
        self.fields.insert(name, QueryField::WithCapacity(capacity));
        self
    }

    pub fn field_with_capacity_and_type(
        mut self,
        name: &str,
        capacity: usize,
        dtype: adt::DataType,
    ) -> Self {
        self.fields
            .insert(name, QueryField::WithCapacityAndType(capacity, dtype));
        self
    }

    pub fn field_with_type(mut self, name: &str, dtype: adt::DataType) -> Self {
        self.fields.insert(name, QueryField::WithType(dtype));
        self
    }

    pub fn aggregate(
        mut self,
        function: AggregateFunction,
        name: Option<String>,
        buffering: QueryField,
    ) -> Self {
        let name = name.unwrap_or(function.aggregate_name());
        self.fields.aggregates.insert(name, (function, buffering));
        self
    }
}

pub struct QueryFieldsBuilderForQuery {
    query_builder: QueryBuilder,
    fields_builder: QueryFieldsBuilder,
}

impl QueryFieldsBuilderForQuery {
    pub(crate) fn new(query_builder: QueryBuilder) -> Self {
        Self {
            query_builder,
            fields_builder: QueryFieldsBuilder::new(),
        }
    }

    pub fn end_fields(self) -> QueryBuilder {
        self.query_builder.with_fields(self.fields_builder.build())
    }

    pub fn field(self, name: &str) -> Self {
        Self {
            fields_builder: self.fields_builder.field(name),
            ..self
        }
    }

    pub fn field_with_buffer(
        self,
        name: &str,
        buffer: Arc<dyn aa::Array>,
    ) -> Self {
        Self {
            fields_builder: self.fields_builder.field_with_buffer(name, buffer),
            ..self
        }
    }

    pub fn field_with_capacity(self, name: &str, capacity: usize) -> Self {
        Self {
            fields_builder: self
                .fields_builder
                .field_with_capacity(name, capacity),
            ..self
        }
    }

    pub fn field_with_capacity_and_type(
        self,
        name: &str,
        capacity: usize,
        dtype: adt::DataType,
    ) -> Self {
        Self {
            fields_builder: self
                .fields_builder
                .field_with_capacity_and_type(name, capacity, dtype),
            ..self
        }
    }

    pub fn field_with_type(self, name: &str, dtype: adt::DataType) -> Self {
        Self {
            fields_builder: self.fields_builder.field_with_type(name, dtype),
            ..self
        }
    }

    pub fn aggregate(
        self,
        function: AggregateFunction,
        name: Option<String>,
        buffering: QueryField,
    ) -> Self {
        Self {
            fields_builder: self
                .fields_builder
                .aggregate(function, name, buffering),
            ..self
        }
    }
}
