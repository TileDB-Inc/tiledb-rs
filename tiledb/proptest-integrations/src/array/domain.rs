/// Test iteration over [Domain] dimensions
fn do_test_dimensions_iter(spec: DomainData) -> TileDBResult<()> {
    let context = Context::new()?;
    let domain = spec.create(&context)?;

    let num_dimensions = domain.num_dimensions()?;
    assert_eq!(num_dimensions, spec.dimension.len());
    assert_eq!(num_dimensions, domain.dimensions()?.count());

    for (dimension_spec, dimension) in
        spec.dimension.iter().zip(domain.dimensions()?)
    {
        let dimension = DimensionData::try_from(dimension?)?;
        assert_option_subset!(dimension_spec, dimension);
    }

    Ok(())
}

proptest! {
    #[test]
    fn test_dimensions_iter(spec in any::<DomainData>()) {
        do_test_dimensions_iter(spec).expect("Error in do_test_dimensions_iter");
    }
}
