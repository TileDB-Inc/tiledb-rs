impl TryFrom<B> for EnumerationData
where
    B: Borrow<Enumeration>,
{
    type Error = crate::error::Error;

    fn try_from(enmr: B) -> TileDBResult<Self> {
        let enmr = enmr.borrow();

        let datatype = enmr.datatype()?;
        let cell_val_num = enmr.cell_val_num()?;
        let data = Box::from(enmr.data()?);
        let offsets: Option<Box<[u64]>> = enmr.offsets()?.map(Box::from);

        Ok(EnumerationData {
            name: enmr.name()?,
            datatype,
            cell_val_num: Some(cell_val_num),
            ordered: Some(enmr.ordered()?),
            data,
            offsets,
        })
    }
}

impl Factory for EnumerationData {
    type Item = Enumeration;

    fn create(&self, context: &Context) -> TileDBResult<Self::Item> {
        let mut b = Builder::new(
            context,
            &self.name,
            self.datatype,
            &self.data[..],
            self.offsets.as_ref().map(|o| &o[..]),
        );

        if let Some(cvn) = self.cell_val_num {
            b = b.cell_val_num(cvn);
        }

        if let Some(ordered) = self.ordered {
            b = b.ordered(ordered);
        }

        b.build()
    }
}
