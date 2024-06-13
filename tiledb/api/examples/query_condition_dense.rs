use std::cell::RefCell;
use std::path::PathBuf;

use itertools::izip;

use tiledb::array::{
    Array, ArrayType, AttributeBuilder, DimensionBuilder, DomainBuilder,
    SchemaBuilder,
};
use tiledb::query::buffer::{BufferMut, QueryBuffersMut};
use tiledb::query::conditions::QueryConditionExpr as QC;
use tiledb::query::{
    Query, QueryBuilder, ReadBuilder, ReadQuery, ReadQueryBuilder, WriteBuilder,
};
use tiledb::{Context, Datatype, Result as TileDBResult};

const ARRAY_URI: &str = "query_condition_dense";
const NUM_ELEMS: i32 = 10;
const C_FILL_VALUE: i32 = -1;
const D_FILL_VALUE: f32 = 0.0;

/// Demonstrate reading dense arrays with query conditions.
fn main() -> TileDBResult<()> {
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let _ = std::env::set_current_dir(
            PathBuf::from(manifest_dir).join("examples").join("output"),
        );
    }

    let ctx = Context::new()?;
    if !Array::exists(&ctx, ARRAY_URI)? {
        create_array(&ctx)?;
        write_array(&ctx)?;
    }

    println!("Reading the entire array:");
    read_array(&ctx, None)?;

    println!("Reading: a is null");
    let qc = QC::field("a").is_null();
    read_array(&ctx, Some(qc))?;

    println!("Reading: b < \"eve\"");
    let qc = QC::field("b").lt("eve");
    read_array(&ctx, Some(qc))?;

    println!("Reading: c >= 1");
    let qc = QC::field("c").ge(1i32);
    read_array(&ctx, Some(qc))?;

    println!("Reading: 3.0 <= d <= 4.0");
    let qc = QC::field("d").ge(3.0f32) & QC::field("d").le(4.0f32);
    read_array(&ctx, Some(qc))?;

    println!("Reading: (a is not null) && (b < \"eve\") && (3.0 <= d <= 4.0)");
    let qc = QC::field("a").not_null()
        & QC::field("b").lt("eve")
        & QC::field("d").ge(3.0f32)
        & QC::field("d").le(4.0f32);
    read_array(&ctx, Some(qc))?;

    Ok(())
}

/// Read the array with the optional query condition and print the results
/// to stdout.
fn read_array(ctx: &Context, qc: Option<QC>) -> TileDBResult<()> {
    let array = tiledb::Array::open(ctx, ARRAY_URI, tiledb::array::Mode::Read)?;
    let mut query = ReadBuilder::new(array)?
        .layout(tiledb::query::QueryLayout::RowMajor)?
        .register_constructor::<_, Vec<i32>>("index", Default::default())?
        .register_constructor::<_, (Vec<i32>, Vec<u8>)>(
            "a",
            Default::default(),
        )?
        .register_constructor::<_, Vec<String>>("b", Default::default())?
        .register_constructor::<_, Vec<i32>>("c", Default::default())?
        .register_constructor::<_, Vec<f32>>("d", Default::default())?
        .start_subarray()?
        .add_range("index", &[0i32, NUM_ELEMS - 1])?
        .finish_subarray()?;

    query = if let Some(qc) = qc {
        query.query_condition(qc)?
    } else {
        query
    };

    let mut query = query.build();

    let (d, (c, (b, ((a, a_validity), (index, ()))))) = query.execute()?;

    for (index, a, a_valid, b, c, d) in izip!(index, a, a_validity, b, c, d) {
        if a_valid == 1 {
            println!("{}: '{}' '{}' '{}', '{}'", index, a, b, c, d)
        } else {
            println!("{}: null '{}', '{}', '{}'", index, b, c, d)
        }
    }
    println!();
    Ok(())
}

/// Function to create the TileDB array used in this example.
/// The array will be 1D with size 1 with dimension "index".
/// The bounds on the index will be 0 through 9, inclusive.
///
/// The array has four attributes. The four attributes are
///  - "a" (type i32)
///  - "b" (type String)
///  - "c" (type i32)
///  - "d" (type f32)
fn create_array(ctx: &Context) -> TileDBResult<()> {
    let domain = {
        let index = DimensionBuilder::new(
            ctx,
            "index",
            Datatype::Int32,
            ([0, NUM_ELEMS - 1], 4),
        )?
        .build();

        DomainBuilder::new(ctx)?.add_dimension(index)?.build()
    };

    let attr_a = AttributeBuilder::new(ctx, "a", Datatype::Int32)?
        .nullability(true)?
        .build();

    let attr_b = AttributeBuilder::new(ctx, "b", Datatype::StringAscii)?
        .var_sized()?
        .build();

    let attr_c = AttributeBuilder::new(ctx, "c", Datatype::Int32)?
        .fill_value(C_FILL_VALUE)?
        .build();

    let attr_d = AttributeBuilder::new(ctx, "d", Datatype::Float32)?
        .fill_value(D_FILL_VALUE)?
        .build();

    let schema = SchemaBuilder::new(ctx, ArrayType::Dense, domain)?
        .add_attribute(attr_a)?
        .add_attribute(attr_b)?
        .add_attribute(attr_c)?
        .add_attribute(attr_d)?
        .build()?;

    Array::create(ctx, ARRAY_URI, schema)
}

/// Write the following data to the array:
///
/// index |  a   |   b   | c |  d
/// -------------------------------
///   0   | null | alice | 0 | 4.1
///   1   | 2    | bob   | 0 | 3.4
///   2   | null | craig | 0 | 5.6
///   3   | 4    | dave  | 0 | 3.7
///   4   | null | erin  | 0 | 2.3
///   5   | 6    | frank | 0 | 1.7
///   6   | null | grace | 1 | 3.8
///   7   | 8    | heidi | 2 | 4.9
///   8   | null | ivan  | 3 | 3.2
///   9   | 10   | judy  | 4 | 3.1
fn write_array(ctx: &Context) -> TileDBResult<()> {
    let a_data = RefCell::new(QueryBuffersMut {
        data: BufferMut::Owned(
            vec![0u32, 2, 0, 4, 0, 6, 0, 8, 0, 10].into_boxed_slice(),
        ),
        cell_structure: Default::default(),
        validity: Some(BufferMut::Owned(
            vec![0u8, 1, 0, 1, 0, 1, 0, 1, 0, 1].into_boxed_slice(),
        )),
    });
    let a_borrowed = a_data.borrow();
    let a_input = a_borrowed.as_shared();
    let b_input = vec![
        "alice", "bob", "craig", "dave", "erin", "frank", "grace", "heidi",
        "ivan", "judy",
    ];
    let c_input = vec![0i32, 0, 0, 0, 0, 0, 1, 2, 3, 4];
    let d_input = vec![4.1f32, 3.4, 5.6, 3.7, 2.3, 1.7, 3.8, 4.9, 3.2, 3.1];

    let array =
        tiledb::Array::open(ctx, ARRAY_URI, tiledb::array::Mode::Write)?;

    let query = WriteBuilder::new(array)?
        .data_typed("a", &a_input)?
        .data_typed("b", &b_input)?
        .data_typed("c", &c_input)?
        .data_typed("d", &d_input)?
        .start_subarray()?
        .add_range("index", &[0i32, NUM_ELEMS - 1])?
        .finish_subarray()?
        .build();

    query.submit().and_then(|_| query.finalize())?;

    Ok(())
}
