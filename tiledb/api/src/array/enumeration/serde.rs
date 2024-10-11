use tiledb_serde::array::enumeration::EnumerationData;

use super::{Builder, Enumeration};
use crate::error::Error as TileDBError;
use crate::{Context, Factory, Result as TileDBResult};

impl TryFrom<&Enumeration> for EnumerationData {
    type Error = TileDBError;

    fn try_from(enmr: &Enumeration) -> Result<Self, Self::Error> {
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

impl TryFrom<Enumeration> for EnumerationData {
    type Error = TileDBError;

    fn try_from(enmr: Enumeration) -> Result<Self, Self::Error> {
        Self::try_from(&enmr)
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
