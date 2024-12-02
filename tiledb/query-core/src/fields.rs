use std::collections::HashMap;
use std::sync::Arc;

use arrow::array as aa;
use arrow::datatypes as adt;
use tiledb_api::query::read::aggregate::AggregateFunction;

use super::QueryBuilder;

#[derive(Debug, thiserror::Error)]
pub enum CapacityNumCellsError {
    #[error("")]
    InvalidFixedSize(i32),
    #[error("")]
    UnsupportedArrowType(adt::DataType),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Capacity {
    /// Request a maximum number of cells of the target field.
    ///
    /// The amount of memory allocated for fixed-length query
    /// fields is the exact amount needed to hold the requested number
    /// of values.
    ///
    /// The amount of space allocated for variable-length query
    /// fields is determined by estimating the size of each variable-length cell.
    Cells(usize),
    /// Request a maximum number of total value of the target field.
    ///
    /// The amount of memory allocated for fixed-length query
    /// fields is the exact amount needed to hold the requested number
    /// of values. This behavior is identical to that of [Self::Cells].
    ///
    /// The amount of memory allocated for variable-length query
    /// fields is the exact amount needed to hold the requested number
    /// of values, plus an additional amount needed to hold an estimated
    /// number of cell offsets.
    Values(usize),
    /// Request whatever fits within a fixed memory limit.
    ///
    /// For variable-length query fields, the fixed memory is apportioned
    /// among the cell values and cell offsets using an estimate for
    /// the average cell size.
    Memory(usize),
}

impl Capacity {
    /// Returns a number of cells of `target_type` which can be held by this capacity.
    ///
    /// For fixed-length target types, the result is exact.
    ///
    /// For variable-length target types, the result is an estimate using an estimated
    /// average number of values per cell.
    ///
    /// Returns `Err` if `target_type` is not a supported [DataType].
    pub fn num_cells(
        &self,
        target_type: &adt::DataType,
        nullable: bool,
    ) -> Result<usize, CapacityNumCellsError> {
        match self {
            Self::Cells(num_cells) => Ok(*num_cells),
            Self::Values(num_values) => {
                calculate_num_cells_by_values(*num_values, target_type)
            }
            Self::Memory(memory_limit) => {
                calculate_by_memory(*memory_limit, target_type, nullable)
                    .map(|(num_cells, _)| num_cells)
            }
        }
    }

    pub fn num_values(
        &self,
        target_type: &adt::DataType,
        nullable: bool,
    ) -> Result<usize, CapacityNumCellsError> {
        match self {
            Self::Cells(num_cells) => {
                calculate_num_values_by_cells(*num_cells, target_type)
            }
            Self::Values(num_values) => Ok(*num_values),
            Self::Memory(memory_limit) => {
                calculate_by_memory(*memory_limit, target_type, nullable)
                    .map(|(_, num_values)| num_values)
            }
        }
    }
}

fn calculate_num_cells_by_values(
    num_values: usize,
    target_type: &adt::DataType,
) -> Result<usize, CapacityNumCellsError> {
    match target_type {
        adt::DataType::FixedSizeBinary(fl) => {
            if *fl < 1 {
                Err(CapacityNumCellsError::InvalidFixedSize(*fl))
            } else {
                Ok(num_values / (*fl as usize))
            }
        }
        adt::DataType::FixedSizeList(ref field, fl) => {
            if *fl < 1 {
                Err(CapacityNumCellsError::InvalidFixedSize(*fl))
            } else {
                let num_elements = num_values / (*fl as usize);
                calculate_num_cells_by_values(num_elements, field.data_type())
            }
        }
        adt::DataType::LargeUtf8
        | adt::DataType::LargeBinary
        | adt::DataType::LargeList(_) => {
            Ok(num_values
                / estimate_average_variable_length_values(target_type))
        }
        _ if target_type.is_primitive() => Ok(num_values),
        _ => todo!(),
    }
}

fn calculate_num_values_by_cells(
    num_cells: usize,
    target_type: &adt::DataType,
) -> Result<usize, CapacityNumCellsError> {
    match target_type {
        adt::DataType::FixedSizeBinary(fl) => {
            if *fl < 1 {
                Err(CapacityNumCellsError::InvalidFixedSize(*fl))
            } else {
                Ok(num_cells * (*fl as usize))
            }
        }
        adt::DataType::FixedSizeList(ref field, fl) => {
            if *fl < 1 {
                Err(CapacityNumCellsError::InvalidFixedSize(*fl))
            } else {
                let num_elements = num_cells * (*fl as usize);
                calculate_num_cells_by_values(num_elements, field.data_type())
            }
        }
        adt::DataType::LargeUtf8
        | adt::DataType::LargeBinary
        | adt::DataType::LargeList(_) => Ok(
            num_cells * estimate_average_variable_length_values(target_type)
        ),
        _ if target_type.is_primitive() => Ok(num_cells),
        _ => todo!(),
    }
}

fn calculate_by_memory(
    memory_limit: usize,
    target_type: &adt::DataType,
    nullable: bool,
) -> Result<(usize, usize), CapacityNumCellsError> {
    match target_type {
        adt::DataType::Boolean => {
            let num_cells = if nullable {
                memory_limit * 8 / 2
            } else {
                memory_limit * 8
            };
            Ok((num_cells, num_cells))
        }
        adt::DataType::LargeList(ref field) => {
            if !field.data_type().is_primitive() {
                return Err(CapacityNumCellsError::UnsupportedArrowType(
                    target_type.clone(),
                ));
            }

            let estimate_values_per_cell =
                estimate_average_variable_length_values(target_type);

            // TODO: Figure out a better way to approximate values to offsets ratios
            // based on whatever Python does or some such.
            //
            // For now, I'll pull a guess at of the ether and assume on average a
            // var sized primitive array averages two values per cell. Becuase why
            // not?
            let width = field.data_type().primitive_width().unwrap();
            let bytes_per_cell = (width * estimate_values_per_cell)
                + std::mem::size_of::<i64>()
                + if nullable { 1 } else { 0 };

            let num_cells = memory_limit / bytes_per_cell;
            let num_values = num_cells * estimate_values_per_cell;
            assert!(
                num_cells
                    * (std::mem::size_of::<i64>()
                        + if nullable { 1 } else { 0 })
                    + num_values * width
                    <= memory_limit
            );

            Ok((num_cells, num_values))
        }
        adt::DataType::FixedSizeList(ref field, cvn) => {
            if !field.data_type().is_primitive() {
                return Err(CapacityNumCellsError::UnsupportedArrowType(
                    target_type.clone(),
                ));
            }

            if *cvn < 1 {
                return Err(CapacityNumCellsError::InvalidFixedSize(*cvn));
            }

            let cvn = *cvn as usize;
            let width = field.data_type().primitive_width().unwrap();
            let bytes_per_cell = memory_limit / (width * cvn);
            let bytes_per_cell = if nullable {
                bytes_per_cell + 1
            } else {
                bytes_per_cell
            };

            let num_cells = memory_limit / bytes_per_cell;
            let num_values = num_cells * cvn;
            assert!(
                num_cells * if nullable { 1 } else { 0 } + num_values * width
                    <= memory_limit
            );
            Ok((num_cells, num_values))
        }
        adt::DataType::LargeUtf8 | adt::DataType::LargeBinary => {
            let values_per_cell =
                estimate_average_variable_length_values(target_type);
            let bytes_per_cell = values_per_cell
                + std::mem::size_of::<i64>()
                + if nullable { 1 } else { 0 };

            let num_cells = memory_limit / bytes_per_cell;
            let num_values = num_cells * values_per_cell;
            assert!(
                num_cells
                    * (std::mem::size_of::<i64>()
                        + if nullable { 1 } else { 0 })
                    + num_values
                    <= memory_limit
            );
            Ok((num_cells, num_values))
        }
        _ if target_type.is_primitive() => {
            let width = target_type.primitive_width().unwrap();
            let bytes_per_cell = width + if nullable { 1 } else { 0 };
            let num_cells = memory_limit / bytes_per_cell;
            let num_values = num_cells;
            Ok((num_cells, num_values))
        }
        _ => Err(CapacityNumCellsError::UnsupportedArrowType(
            target_type.clone(),
        )
        .into()),
    }
}

/// Returns a guess for how many variable-length values the average cell has.
fn estimate_average_variable_length_values(
    target_type: &adt::DataType,
) -> usize {
    // A bad value here will lead to poor memory utilization.
    // - if this estimate is too small then the results will fill up the variable-length
    //   data buffers quickly, and the fixed-size data buffers will be under-utilized.
    // - if this estimate is too large, then the results will fill up the fixed-length
    //   data buffers quickly, and the variable-size data buffers will be under-utilized.
    //
    // Some ideas core could implement to improve this estimate:
    // - keep a histogram of average cell length in fragment metadata
    // - register a single buffer for all variable-length data values
    // - write offsets and variable-length data into a single buffer, writing the fixed-size offsets
    //   in order from the front and the variable-size values in reverse cell order from the back
    //   (the result buffer is full when the two would meet in the middle)
    // - produce results in row-major order and write the variable-length parts
    //   for all query fields in reverse from the end of the buffer, similar to the above
    match target_type {
        adt::DataType::LargeUtf8 => {
            // https://datafusion.apache.org/blog/2024/09/13/string-view-german-style-strings-part-1/
            // "German" strings have a buffer for 16 bytes which optimizes access for strings
            // which are 12 bytes and shorter.
            //
            // https://www.vldb.org/pvldb/vol17/p148-zeng.pdf
            // claims that in real-world datasets 99% of strings are of length 128 or less.
            //
            // But of course what makes sense is domain-specific.
            // A username is probably short, an email is probably longer than this.
            16
        }
        adt::DataType::LargeBinary => {
            // this can be literally anything, so go with 1KiB?
            1024
        }
        adt::DataType::LargeList(_) => {
            // also pulling a number out of thin air
            4
        }
        _ => unreachable!(),
    }
}

/// The default capacity for a field is 10MiB.
///
/// Use of this default is not recommended for queries which request
/// multiple fields of different fixed sizes. Except where aggregates
/// are concerned, queries return the same number of cells for each
/// target field. This means that the number of cells returned by a
/// query is bounded by the number of cells which fit in the buffer
/// allocated for the largest field. If the buffers for each field are
/// the same size, then buffers for smaller fields will not be fully
/// utilized.
///
/// For example, a query to a `Datatype::Int32` field and a
/// `Datatype::Int64` field which writes to a 12MiB buffer per field
/// can write only up to 1.5M cells per submit. This fully utilizes
/// the `Datatype::Int64` buffer but only utilizes 50% of the
/// `Datatype::Int32` buffer. A better strategy would be to allocate
/// twice as much memory for the `Datatype::Int64` field as for
/// the `Datatype::Int32` field, such as by using [Self::Cells].
///
/// Note that [Self::Values] is not the default for similar reasons,
/// and [Self::Cells] is not the default to avoid large fields
/// from using unexpectedly large amounts of memory.
impl Default for Capacity {
    fn default() -> Self {
        Self::Memory(1024 * 1024 * 10)
    }
}

#[derive(Debug, Default)]
pub enum QueryField {
    #[default]
    Default,
    WithCapacity(Capacity),
    WithCapacityAndType(Capacity, adt::DataType),
    WithType(adt::DataType),
    Buffer(Arc<dyn aa::Array>),
}

impl QueryField {
    pub fn capacity(&self) -> Option<Capacity> {
        match self {
            Self::Default => Some(Default::default()),
            Self::WithCapacity(capacity) => Some(*capacity),
            Self::WithCapacityAndType(capacity, _) => Some(*capacity),
            Self::WithType(_) => Some(Default::default()),
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

    pub fn field_with_capacity(
        mut self,
        name: &str,
        capacity: Capacity,
    ) -> Self {
        self.fields.insert(name, QueryField::WithCapacity(capacity));
        self
    }

    pub fn field_with_capacity_and_type(
        mut self,
        name: &str,
        capacity: Capacity,
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

    pub fn field_with_capacity(self, name: &str, capacity: Capacity) -> Self {
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
        capacity: Capacity,
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
