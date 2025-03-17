use std::collections::HashMap;
use std::rc::Rc;

use cells::write::{
    DenseWriteInput, SparseWriteInput, WriteInput, WriteInputRef,
};
use cells::{typed_field_data_go, Cells, FieldData};
use tiledb_common::array::{ArrayType, CellValNum};
use tiledb_common::physical_type_go;
use tiledb_pod::array::attribute::strategy::Requirements as AttributeRequirements;
use tiledb_pod::array::dimension::strategy::Requirements as DimensionRequirements;
use tiledb_pod::array::domain::strategy::Requirements as DomainRequirements;
use tiledb_pod::array::schema::strategy::Requirements as SchemaRequirements;
use tiledb_pod::filter::strategy::Requirements as FilterRequirements;

use super::*;
use crate::query::read::output::{
    CellStructureSingleIterator, FixedDataIterator, RawReadOutput,
    TypedRawReadOutput, VarDataIterator,
};
use crate::query::read::{
    CallbackVarArgReadBuilder, FieldMetadata, ManagedBuffer, Map, MapAdapter,
    RawReadHandle, ReadCallbackVarArg, TypedReadHandle,
};
use crate::typed_query_buffers_go;

/// Returns a base set of requirements for filters to be used
/// in write queries.
///
/// Requirements are chosen to either avoid
/// constraints on input (e.g. positive delta filtering requires
/// sorted input, float scale filtering is not invertible)
/// or to avoid issues in the tiledb core library in as
/// many scenarios as possible.
// now that we're actually writing data we will hit the fun bugs.
// there are several in the filter pipeline, so we must heavily
// restrict what is allowed until the bugs are fixed.
pub fn query_write_filter_requirements() -> FilterRequirements {
    FilterRequirements {
        allow_bit_reduction: false,     // SC-47560
        allow_bit_shuffle: false,       // SC-48409
        allow_byte_shuffle: false,      // SC-48409
        allow_positive_delta: false,    // nothing yet to ensure sort order
        allow_scale_float: false,       // not invertible due to precision loss
        allow_xor: false,               // SC-47328
        allow_compression_rle: false, // probably can be enabled but nontrivial
        allow_compression_dict: false, // probably can be enabled but nontrivial
        allow_compression_delta: false, // SC-47328
        allow_webp: false,            // SC-51250
        ..Default::default()
    }
}

/// Returns a base set of schema requirements for running a query.
///
/// Requirements are chosen to either avoid constraints on write input
/// or to avoid issues in the tiledb core library in as many scenarios as possible.
pub fn query_write_schema_requirements(
    array_type: Option<ArrayType>,
) -> SchemaRequirements {
    // NB: 1 is the highest number that passes all cases (so don't use the value given by
    // `DomainRequirements::default()`) but we want to enable environmental override.
    let env_max_dimensions =
        DomainRequirements::env_max_dimensions().unwrap_or(1);

    SchemaRequirements {
        domain: Some(Rc::new(DomainRequirements {
            array_type,
            num_dimensions: 1..=env_max_dimensions,
            dimension: Some(DimensionRequirements {
                filters: Some(Rc::new(query_write_filter_requirements())),
                ..Default::default()
            }),
            ..Default::default()
        })),
        attributes: Some(AttributeRequirements {
            filters: Some(Rc::new(query_write_filter_requirements())),
            ..Default::default()
        }),
        coordinates_filters: Some(Rc::new(query_write_filter_requirements())),
        offsets_filters: Some(Rc::new(query_write_filter_requirements())),
        validity_filters: Some(Rc::new(query_write_filter_requirements())),
        ..Default::default()
    }
}

impl From<&TypedRawReadOutput<'_>> for FieldData {
    fn from(value: &TypedRawReadOutput) -> Self {
        typed_query_buffers_go!(value.buffers, DT, ref handle, {
            let rr = RawReadOutput {
                ncells: value.ncells,
                input: handle.borrow(),
            };
            match rr.input.cell_structure.as_cell_val_num() {
                CellValNum::Fixed(nz) if nz.get() == 1 => Self::from(
                    CellStructureSingleIterator::try_from(rr)
                        .unwrap()
                        .collect::<Vec<DT>>(),
                ),
                CellValNum::Fixed(_) => Self::from(
                    FixedDataIterator::try_from(rr)
                        .unwrap()
                        .map(|slice| slice.to_vec())
                        .collect::<Vec<Vec<DT>>>(),
                ),
                CellValNum::Var => Self::from(
                    VarDataIterator::try_from(rr)
                        .unwrap()
                        .map(|s| s.to_vec())
                        .collect::<Vec<Vec<DT>>>(),
                ),
            }
        })
    }
}

impl ToReadQuery for Cells {
    type ReadBuilder<'data, B> =
        CallbackVarArgReadBuilder<'data, RawResultCallback, B>;

    fn attach_read<'data, B>(
        &self,
        b: B,
    ) -> TileDBResult<Self::ReadBuilder<'data, B>>
    where
        B: ReadQueryBuilder<'data>,
    {
        let field_order = self.fields().keys().cloned().collect::<Vec<_>>();
        let handles = {
            let schema = b.base().array().schema().unwrap();

            field_order
                .iter()
                .map(|name| {
                    let field = schema.field(name.clone()).unwrap();
                    physical_type_go!(field.datatype().unwrap(), DT, {
                        let managed: ManagedBuffer<DT> = ManagedBuffer::new(
                            field.query_scratch_allocator(None).unwrap(),
                        );
                        let metadata = FieldMetadata::try_from(&field).unwrap();
                        let rr = RawReadHandle::managed(metadata, managed);
                        TypedReadHandle::from(rr)
                    })
                })
                .collect::<Vec<TypedReadHandle>>()
        };

        b.register_callback_var(handles, RawResultCallback { field_order })
    }
}

impl ToReadQuery for DenseWriteInput {
    type ReadBuilder<'data, B>
        = CallbackVarArgReadBuilder<
        'data,
        MapAdapter<CellsConstructor, RawResultCallback>,
        B,
    >
    where
        Self: 'data;

    fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<Self::ReadBuilder<'data, B>>
    where
        B: ReadQueryBuilder<'data>,
    {
        let mut subarray = b.start_subarray()?;

        for i in 0..self.subarray.len() {
            subarray = subarray.add_range(i, self.subarray[i].clone())?;
        }

        let b: B = subarray.finish_subarray()?.layout(self.layout)?;

        Ok(self.data.attach_read(b)?.map(CellsConstructor::new()))
    }
}

impl ToReadQuery for SparseWriteInput {
    type ReadBuilder<'data, B> = CallbackVarArgReadBuilder<
        'data,
        MapAdapter<CellsConstructor, RawResultCallback>,
        B,
    >;

    fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<Self::ReadBuilder<'data, B>>
    where
        B: ReadQueryBuilder<'data>,
    {
        Ok(self.data.attach_read(b)?.map(CellsConstructor::new()))
    }
}

impl ToReadQuery for WriteInput {
    type ReadBuilder<'data, B> = CallbackVarArgReadBuilder<
        'data,
        MapAdapter<CellsConstructor, RawResultCallback>,
        B,
    >;

    fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<Self::ReadBuilder<'data, B>>
    where
        B: ReadQueryBuilder<'data>,
    {
        match self {
            Self::Dense(ref d) => d.attach_read(b),
            Self::Sparse(ref s) => s.attach_read(b),
        }
    }
}

impl ToReadQuery for WriteInputRef<'_> {
    type ReadBuilder<'data, B>
        = CallbackVarArgReadBuilder<
        'data,
        MapAdapter<CellsConstructor, RawResultCallback>,
        B,
    >
    where
        Self: 'data;

    fn attach_read<'data, B>(
        &'data self,
        b: B,
    ) -> TileDBResult<Self::ReadBuilder<'data, B>>
    where
        B: ReadQueryBuilder<'data>,
    {
        match self {
            Self::Dense(d) => d.attach_read(b),
            Self::Sparse(s) => s.attach_read(b),
        }
    }
}

impl ToWriteQuery for Cells {
    fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        let mut b = b;
        for f in self.fields().iter() {
            b = typed_field_data_go!(f.1, data, b.data_typed(f.0, data))?;
        }
        Ok(b)
    }
}

impl ToWriteQuery for DenseWriteInput {
    fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        let mut subarray = self.data.attach_write(b)?.start_subarray()?;

        for i in 0..self.subarray.len() {
            subarray = subarray.add_range(i, self.subarray[i].clone())?;
        }

        subarray.finish_subarray()?.layout(self.layout)
    }
}

impl ToWriteQuery for SparseWriteInput {
    fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        self.data.attach_write(b)
    }
}

impl ToWriteQuery for WriteInput {
    fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        match self {
            Self::Dense(ref d) => d.attach_write(b),
            Self::Sparse(ref s) => s.attach_write(b),
        }
    }
}

impl ToWriteQuery for WriteInputRef<'_> {
    fn attach_write<'data>(
        &'data self,
        b: WriteBuilder<'data>,
    ) -> TileDBResult<WriteBuilder<'data>> {
        match self {
            Self::Dense(d) => d.attach_write(b),
            Self::Sparse(s) => s.attach_write(b),
        }
    }
}

// TODO: where should these go
pub struct RawReadQueryResult(pub HashMap<String, FieldData>);

pub struct RawResultCallback {
    pub field_order: Vec<String>,
}

impl ReadCallbackVarArg for RawResultCallback {
    type Intermediate = RawReadQueryResult;
    type Final = RawReadQueryResult;
    type Error = std::convert::Infallible;

    fn intermediate_result(
        &mut self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Intermediate, Self::Error> {
        Ok(RawReadQueryResult(
            self.field_order
                .iter()
                .zip(args.iter())
                .map(|(f, a)| (f.clone(), FieldData::from(a)))
                .collect::<HashMap<String, FieldData>>(),
        ))
    }

    fn final_result(
        mut self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Intermediate, Self::Error> {
        self.intermediate_result(args)
    }
}

/// Query callback which accumulates results from each step into `Cells`
/// and returns the `Cells` as the final result.
#[derive(Default)]
pub struct CellsConstructor {
    cells: Option<Cells>,
}

impl CellsConstructor {
    pub fn new() -> Self {
        CellsConstructor { cells: None }
    }
}

impl Map<RawReadQueryResult, RawReadQueryResult> for CellsConstructor {
    type Intermediate = ();
    type Final = Cells;

    fn map_intermediate(
        &mut self,
        batch: RawReadQueryResult,
    ) -> Self::Intermediate {
        let batch = Cells::new(batch.0);
        if let Some(cells) = self.cells.as_mut() {
            cells.extend(batch);
        } else {
            self.cells = Some(batch)
        }
    }

    fn map_final(mut self, batch: RawReadQueryResult) -> Self::Final {
        self.map_intermediate(batch);
        self.cells.unwrap()
    }
}

#[cfg(test)]
mod tests;
