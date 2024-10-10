impl TryFrom<B> for DomainData
where
    B: Borrow<Domain>,
{
    type Error = crate::error::Error;

    fn try_from(domain: B) -> TileDBResult<Self> {
        let domain = domain.borrow();
        Ok(DomainData {
            dimension: (0..domain.num_dimensions()?)
                .map(|d| DimensionData::try_from(&domain.dimension(d)?))
                .collect::<TileDBResult<Vec<DimensionData>>>()?,
        })
    }
}

impl Factory for DomainData {
    type Item = Domain;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        Ok(self
            .dimension
            .iter()
            .try_fold(Builder::new(context)?, |b, d| {
                b.add_dimension(d.create(context)?)
            })?
            .build())
    }
}
