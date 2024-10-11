#[test]
fn dimension_ranges() {
    let ctx = Context::new().unwrap();

    let test_uri = tiledb_test_utils::get_uri_generator()
        .map_err(|e| Error::Other(e.to_string()))
        .unwrap();
    let test_uri =
        crate::array::tests::create_quickstart_sparse_string(&test_uri, &ctx)
            .unwrap();

    let derive_att = |row: &str, col: &i32| -> i32 {
        let mut h = DefaultHasher::new();
        (row, col).hash(&mut h);
        h.finish() as i32
    };

    let row_values = ["foo", "bar", "baz", "quux", "gub"];
    let col_values = (1..=4).collect::<Vec<i32>>();

    // write some data
    {
        let (rows, (cols, atts)) = row_values
            .iter()
            .flat_map(|r| {
                col_values.iter().map(move |c| {
                    let att = derive_att(r, c);
                    (r.to_string(), (*c, att))
                })
            })
            .collect::<(Vec<String>, (Vec<i32>, Vec<i32>))>();

        let w = Array::open(&ctx, &test_uri, Mode::Write).unwrap();
        let q = WriteBuilder::new(w)
            .unwrap()
            .data("rows", &rows)
            .unwrap()
            .data("cols", &cols)
            .unwrap()
            .data("a", &atts)
            .unwrap()
            .build();

        q.submit().unwrap();
        q.finalize().unwrap();
    }

    let schema = {
        let array = Array::open(&ctx, &test_uri, Mode::Read).unwrap();
        Rc::new(SchemaData::try_from(array.schema().unwrap()).unwrap())
    };
    let do_dimension_ranges = |subarray: SubarrayData| -> TileDBResult<()> {
        let array = Array::open(&ctx, &test_uri, Mode::Read).unwrap();
        let mut q = ReadBuilder::new(array)?
            .start_subarray()?
            .dimension_ranges(subarray.dimension_ranges.clone())?
            .finish_subarray()?
            .register_constructor::<_, Vec<String>>("rows", Default::default())?
            .register_constructor::<_, Vec<i32>>("cols", Default::default())?
            .register_constructor::<_, Vec<i32>>("a", Default::default())?
            .build();

        let (atts, (cols, (rows, _))) = q.execute()?;
        assert_eq!(rows.len(), cols.len());
        assert_eq!(rows.len(), atts.len());

        // validate the number of results.
        // this is hard to do with multi ranges which might be overlapping
        // so skip for those cases. tiledb returns the union of subarray
        // ranges by default, so to be accurate we would have to do the union
        if subarray.dimension_ranges[0].len() <= 1
            && subarray.dimension_ranges[1].len() <= 1
        {
            let num_cells_0 = if subarray.dimension_ranges[0].is_empty() {
                row_values.len()
            } else {
                let Range::Var(VarValueRange::UInt8(ref lb, ref ub)) =
                    subarray.dimension_ranges[0][0]
                else {
                    unreachable!()
                };
                row_values
                    .iter()
                    .filter(|row| {
                        lb.as_ref() <= row.as_bytes()
                            && row.as_bytes() <= ub.as_ref()
                    })
                    .count()
            };
            let num_cells_1 = if subarray.dimension_ranges[1].is_empty() {
                col_values.len()
            } else {
                subarray.dimension_ranges[1][0].num_cells().unwrap() as usize
            };

            let expect_num_cells = num_cells_0 * num_cells_1;
            assert_eq!(expect_num_cells, rows.len());
        }
        for (row, col, att) in izip!(rows, cols, atts) {
            assert_eq!(att, derive_att(&row, &col));

            let row_in_bounds = subarray.dimension_ranges[0].is_empty()
                || subarray.dimension_ranges[0].iter().any(|r| {
                    let Range::Var(VarValueRange::UInt8(ref lb, ref ub)) = r
                    else {
                        unreachable!()
                    };
                    lb.as_ref() <= row.as_bytes()
                        && row.as_bytes() <= ub.as_ref()
                });
            assert!(row_in_bounds);

            let col_in_bounds = subarray.dimension_ranges[1].is_empty()
                || subarray.dimension_ranges[1].iter().any(|r| {
                    let Range::Single(SingleValueRange::Int32(lb, ub)) = r
                    else {
                        unreachable!()
                    };
                    *lb <= col && col <= *ub
                });
            assert!(col_in_bounds);
        }

        Ok(())
    };

    proptest!(move |(subarray in any_with::<SubarrayData>(Some(Rc::clone(&schema))))| {
        do_dimension_ranges(subarray).expect("Read query error");
    })
}
