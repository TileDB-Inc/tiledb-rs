impl TryFrom<B> for DimensionData
where
    B: Borrow<Dimension>,
{
    type Error = crate::error::Error;

    fn try_from(dim: B) -> TileDBResult<Self> {
        let dim = dim.borrow();
        let datatype = dim.datatype()?;
        let constraints = physical_type_go!(datatype, DT, {
            let domain = dim.domain::<DT>()?;
            let extent = dim.extent::<DT>()?;
            if let Some(domain) = domain {
                DimensionConstraints::from((domain, extent))
            } else {
                assert!(extent.is_none());
                DimensionConstraints::StringAscii
            }
        });

        Ok(DimensionData {
            name: dim.name()?,
            datatype,
            constraints,
            filters: {
                let fl = FilterListData::try_from(&dim.filters()?)?;
                if fl.is_empty() {
                    None
                } else {
                    Some(fl)
                }
            },
        })
    }
}

impl Factory for DimensionData {
    type Item = Dimension;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        let mut b = Builder::new(
            context,
            &self.name,
            self.datatype,
            self.constraints.clone(),
        )?;

        if let Some(fl) = self.filters.as_ref() {
            b = b.filters(fl.create(context)?)?;
        }

        Ok(b.cell_val_num(self.cell_val_num())?.build())
    }
}
