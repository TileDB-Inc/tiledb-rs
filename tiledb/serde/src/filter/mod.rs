#[cfg(feature = "api-conversions")]
pub mod conversions {
    impl TryFrom<B> for FilterData
    where
        B: Borrow<Filter>,
    {
        type Error = crate::error::Error;

        fn try_from(filter: B) -> TileDBResult<Self> {
            filter.borrow().filter_data()
        }
    }

    impl crate::Factory for FilterData {
        type Item = Filter;

        fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
            Filter::create(context, self)
        }
    }

    impl TryFrom<B> for Vec<FilterData>
    where
        B: Borrow<FilterPipeline>,
    {
        type Error = !;

        fn try_from(pipeline: B) -> TileDBResult<Self> {
            pipeline
                .borrow()
                .iter()
                .map(|f| FilterData::try_from(f))
                .collect::<Result<Self, Self::Error>>()
        }
    }

    impl crate::Factory for Vec<FilterData> {
        type Item = FilterPipeline;

        fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
            Ok(self
                .iter()
                .fold(FilterPipelineBuilder::new(context), |b, filter| {
                    b?.add_filter_data(filter)
                })?
                .build())
        }
    }
}
