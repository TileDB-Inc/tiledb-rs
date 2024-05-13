extern crate tiledb;

use std::cell::{Ref, RefCell};

use itertools::izip;
use tiledb::array::{CellOrder, TileOrder};
use tiledb::query::buffer::{
    BufferMut, CellStructureMut, QueryBuffers, QueryBuffersMut,
};
use tiledb::query::read::output::{NonVarSized, VarDataIterator, VarSized};
use tiledb::query::read::{FnMutAdapter, ReadStepOutput, ScratchStrategy};
use tiledb::query::{
    Query, QueryBuilder, ReadBuilder, ReadQuery, ReadQueryBuilder,
};
use tiledb::Datatype;
use tiledb::Result as TileDBResult;

const ARRAY_NAME: &str = "reading_incomplete_array";

const INT32_ATTRIBUTE_NAME: &str = "a1";
const CHAR_ATTRIBUTE_NAME: &str = "a2";

/// Returns whether the example array already exists
fn array_exists() -> bool {
    let tdb = match tiledb::Context::new() {
        Err(_) => return false,
        Ok(tdb) => tdb,
    };

    tiledb::array::Array::exists(&tdb, ARRAY_NAME)
        .expect("Error checking array existence")
}

/// Creates a dense array at URI `ARRAY_NAME`.
/// The array has two i32 dimensions ["rows", "columns"] with two
/// attributes in each cell - (a1 INT32, a2 CHAR).
/// Both "rows" and "columns" dimensions range from 1 to 4, and the tiles
/// span all 4 elements on each dimension.
/// Hence we have 16 cells of data and a single tile for the whole array.
fn create_array() -> TileDBResult<()> {
    let tdb = tiledb::Context::new()?;

    let domain = {
        let rows: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new(
                &tdb,
                "rows",
                Datatype::Int32,
                ([1, 4], 4),
            )?
            .build();
        let cols: tiledb::array::Dimension =
            tiledb::array::DimensionBuilder::new(
                &tdb,
                "columns",
                Datatype::Int32,
                ([1, 4], 4),
            )?
            .build();

        tiledb::array::DomainBuilder::new(&tdb)?
            .add_dimension(rows)?
            .add_dimension(cols)?
            .build()
    };

    let attribute_int32 = tiledb::array::AttributeBuilder::new(
        &tdb,
        INT32_ATTRIBUTE_NAME,
        tiledb::Datatype::Int32,
    )?
    .build();

    let attribute_char = tiledb::array::AttributeBuilder::new(
        &tdb,
        CHAR_ATTRIBUTE_NAME,
        tiledb::Datatype::Char,
    )?
    .var_sized()?
    .build();

    let schema = tiledb::array::SchemaBuilder::new(
        &tdb,
        tiledb::array::ArrayType::Sparse,
        domain,
    )?
    .cell_order(CellOrder::RowMajor)?
    .tile_order(TileOrder::RowMajor)?
    .add_attribute(attribute_int32)?
    .add_attribute(attribute_char)?
    .build()?;

    tiledb::Array::create(&tdb, ARRAY_NAME, schema)
}

/// Writes data into the array.
/// After the write, the contents of the array will be:
/// [[ (1, "a"), (2, "bb"),  _, _],
///  [ _,        (3, "ccc"), _, _],
///  [ _,        _,          _, _],
///  [ _,        _,          _, _]]
fn write_array() -> TileDBResult<()> {
    let tdb = tiledb::Context::new()?;

    let array =
        tiledb::Array::open(&tdb, ARRAY_NAME, tiledb::array::Mode::Write)?;

    let coords_rows = vec![1, 2, 2];
    let coords_cols = vec![1, 1, 2];

    let int32_data = vec![1, 2, 3];
    let char_data = vec!["a", "bb", "ccc"];

    let query = tiledb::query::WriteBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::Global)?
        .data_typed("rows", &coords_rows)?
        .data_typed("columns", &coords_cols)?
        .data_typed(INT32_ATTRIBUTE_NAME, &int32_data)?
        .data_typed(CHAR_ATTRIBUTE_NAME, &char_data)?
        .build();

    query.submit().and_then(|_| query.finalize())?;
    Ok(())
}

/// The goal of this is example is to demonstrate handling incomplete results
/// from a query.  The example wants to print out the query result set.
/// Below are several different ways to implement this functionality.

fn query_builder_start(tdb: &tiledb::Context) -> TileDBResult<ReadBuilder> {
    let array =
        tiledb::Array::open(tdb, ARRAY_NAME, tiledb::array::Mode::Read)?;

    tiledb::query::ReadBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .start_subarray()?
        .add_range(0, &[1i32, 4])?
        .add_range(1, &[1i32, 4])?
        .finish_subarray()
}

fn grow_buffer<T>(b: &mut BufferMut<T>)
where
    T: Copy + Default,
{
    let capacity = b.as_ref().len();
    let new_capacity = 2 * capacity;
    let _ = std::mem::replace(
        b,
        BufferMut::Owned(vec![T::default(); new_capacity].into_boxed_slice()),
    );
}

/// Handles the incomplete results manually, similar to what might be done with the C API.
/// Buffers are provided for the query to fill in. Each step fills in as much
/// as it can, we print the results and then re-submit the query.
/// The initial buffer sizes are deliberately small to force the NotEnoughSpace
/// result, which is handled manually by swapping the buffer inside of the RefCell.
fn read_array_step() -> TileDBResult<()> {
    println!("read_array_step");

    let init_capacity = 1;

    let tdb = tiledb::context::Context::new()?;

    let rows_output = RefCell::new(QueryBuffersMut {
        data: BufferMut::Owned(vec![0i32; init_capacity].into_boxed_slice()),
        cell_structure: Default::default(),
        validity: None,
    });
    let cols_output = RefCell::new(QueryBuffersMut {
        data: BufferMut::Owned(vec![0i32; init_capacity].into_boxed_slice()),
        cell_structure: Default::default(),
        validity: None,
    });
    let int32_output = RefCell::new(QueryBuffersMut {
        data: BufferMut::Owned(vec![0i32; init_capacity].into_boxed_slice()),
        cell_structure: Default::default(),
        validity: None,
    });
    let char_output = RefCell::new(QueryBuffersMut {
        data: BufferMut::Owned(vec![0u8; init_capacity].into_boxed_slice()),
        cell_structure: CellStructureMut::Var(BufferMut::Owned(
            vec![0u64; init_capacity].into_boxed_slice(),
        )),
        validity: None,
    });

    let mut qq = query_builder_start(&tdb)?
        .register_raw("rows", &rows_output)?
        .register_raw("columns", &cols_output)?
        .register_raw(INT32_ATTRIBUTE_NAME, &int32_output)?
        .register_raw(CHAR_ATTRIBUTE_NAME, &char_output)?
        .build();

    loop {
        let res = qq.step()?;
        let final_result = res.is_final();

        if let Some((n_a2, (n_a1, (n_cols, (n_rows, _))))) = res.into_inner() {
            let rows =
                Ref::map(rows_output.borrow(), |o| &o.data.as_ref()[0..n_rows]);
            let cols =
                Ref::map(cols_output.borrow(), |o| &o.data.as_ref()[0..n_cols]);
            let a1 =
                Ref::map(int32_output.borrow(), |o| &o.data.as_ref()[0..n_a1]);

            let char_output: Ref<QueryBuffersMut<u8>> = char_output.borrow();
            let char_output: QueryBuffers<u8> = char_output.as_shared();
            let a2 = VarDataIterator::new(n_a2, char_output)
                .expect("Expected variable data offsets")
                .map(|bytes| String::from_utf8_lossy(bytes).to_string());

            for (row, column, a1, a2) in
                izip!(rows.iter(), cols.iter(), a1.iter(), a2)
            {
                println!("\tCell ({}, {}) a1: {}, a2: {}", row, column, a1, a2)
            }
        } else {
            println!("\t\tNot enough space, growing buffers...");
            grow_buffer(&mut rows_output.borrow_mut().data);
            grow_buffer(&mut cols_output.borrow_mut().data);
            grow_buffer(&mut int32_output.borrow_mut().data);
            grow_buffer(&mut char_output.borrow_mut().data);
            grow_buffer(
                char_output
                    .borrow_mut()
                    .cell_structure
                    .offsets_mut()
                    .unwrap(),
            );
        }

        if final_result {
            break;
        }
    }

    Ok(())
}

/// Ignores the details of incomplete results by collecting them into a result set
/// and then printing the result set.  Capacities for each attribute are deliberately
/// small in this example to force the NotEnoughSpace result, which is handled
/// inside of `execute`.
fn read_array_collect() -> TileDBResult<()> {
    println!("read_array_collect");

    let tdb = tiledb::context::Context::new()?;

    let mut qq = query_builder_start(&tdb)?
        .register_constructor::<_, Vec<i32>>(
            "rows",
            ScratchStrategy::CustomAllocator(Box::new(NonVarSized {
                capacity: 1,
                ..Default::default()
            })),
        )?
        .register_constructor::<_, Vec<i32>>(
            "columns",
            ScratchStrategy::CustomAllocator(Box::new(NonVarSized {
                capacity: 1,
                ..Default::default()
            })),
        )?
        .register_constructor::<_, Vec<i32>>(
            INT32_ATTRIBUTE_NAME,
            ScratchStrategy::CustomAllocator(Box::new(NonVarSized {
                capacity: 1,
                ..Default::default()
            })),
        )?
        .register_constructor::<_, Vec<String>>(
            CHAR_ATTRIBUTE_NAME,
            ScratchStrategy::CustomAllocator(Box::new(VarSized {
                byte_capacity: 1,
                offset_capacity: 1,
            })),
        )?
        .build();

    let (a2, (a1, (column, (row, _)))) = qq.execute()?;

    for (row, column, a1, a2) in izip!(row, column, a1, a2) {
        println!("\tCell ({}, {}) a1: {}, a2: {}", row, column, a1, a2)
    }

    Ok(())
}

/// Ignores the details of incomplete results by register a callback to run
/// on each record which prints the result set.  Capacities for each attribute
/// are deliberately small in this example to force the NotEnoughSpace result,
/// which is handled manually by swapping the buffer inside of the RefCell.
fn read_array_callback() -> TileDBResult<()> {
    fn callback(row: i32, column: i32, a1: i32, a2: String) {
        println!("\tCell ({}, {}) a1: {}, a2: {}", row, column, a1, a2)
    }

    println!("read_array_callback");

    let init_capacity = 1;

    let tdb = tiledb::context::Context::new()?;

    let rows_output = RefCell::new(QueryBuffersMut {
        data: BufferMut::Owned(vec![0i32; init_capacity].into_boxed_slice()),
        cell_structure: Default::default(),
        validity: None,
    });
    let cols_output = RefCell::new(QueryBuffersMut {
        data: BufferMut::Owned(vec![0i32; init_capacity].into_boxed_slice()),
        cell_structure: Default::default(),
        validity: None,
    });
    let int32_output = RefCell::new(QueryBuffersMut {
        data: BufferMut::Owned(vec![0i32; init_capacity].into_boxed_slice()),
        cell_structure: Default::default(),
        validity: None,
    });
    let char_output = RefCell::new(QueryBuffersMut {
        data: BufferMut::Owned(vec![0u8; init_capacity].into_boxed_slice()),
        cell_structure: CellStructureMut::Var(BufferMut::Owned(
            vec![0u64; init_capacity].into_boxed_slice(),
        )),
        validity: None,
    });
    let mut qq = query_builder_start(&tdb)?
        .register_callback4::<FnMutAdapter<(i32, i32, i32, String), _>>(
            ("rows", ScratchStrategy::RawBuffers(&rows_output)),
            ("columns", ScratchStrategy::RawBuffers(&cols_output)),
            (
                INT32_ATTRIBUTE_NAME,
                ScratchStrategy::RawBuffers(&int32_output),
            ),
            (
                CHAR_ATTRIBUTE_NAME,
                ScratchStrategy::RawBuffers(&char_output),
            ),
            FnMutAdapter::new(callback),
        )?
        .build();

    loop {
        let res = qq.step()?;

        match res {
            ReadStepOutput::NotEnoughSpace => {
                println!("\t\tNot enough space, growing buffers...");
                grow_buffer(&mut rows_output.borrow_mut().data);
                grow_buffer(&mut cols_output.borrow_mut().data);
                grow_buffer(&mut int32_output.borrow_mut().data);
                grow_buffer(&mut char_output.borrow_mut().data);
                grow_buffer(
                    char_output
                        .borrow_mut()
                        .cell_structure
                        .offsets_mut()
                        .unwrap(),
                );
            }
            ReadStepOutput::Intermediate(_) => {}
            ReadStepOutput::Final(_) => break,
        }
    }
    Ok(())
}

fn main() -> TileDBResult<()> {
    if !array_exists() {
        create_array().expect("Failed to create array");
        write_array().expect("Failed to write array");
    }
    read_array_step().expect("Failed to step through array results");
    read_array_collect().expect("Failed to collect array results");
    read_array_callback().expect("Failed to apply callback to array results");
    Ok(())
}
