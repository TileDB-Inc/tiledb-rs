use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::{Range, RangeInclusive};
use std::rc::Rc;

use paste::paste;
use proptest::bits::{BitSetLike, VarBitSet};
use proptest::collection::SizeRange;
use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;
use tiledb_test_utils::strategy::records::{Records, RecordsValueTree};

use crate::array::schema::FieldData as SchemaField;
use crate::array::{ArrayType, CellValNum, SchemaData};
use crate::datatype::physical::{BitsEq, BitsOrd, IntegralType};
use crate::error::Error;
use crate::query::read::output::{
    CellStructureSingleIterator, FixedDataIterator, RawReadOutput,
    TypedRawReadOutput, VarDataIterator,
};
use crate::query::read::{
    CallbackVarArgReadBuilder, FieldMetadata, ManagedBuffer, Map,
    RawReadHandle, ReadCallbackVarArg, ReadQueryBuilder, TypedReadHandle,
};
use crate::query::WriteBuilder;
use crate::{
    dimension_constraints_go, physical_type_go, typed_query_buffers_go,
    Datatype, Result as TileDBResult,
};

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
