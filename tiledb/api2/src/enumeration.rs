use std::pin::Pin;

use tiledb_sys2::buffer::Buffer;
use tiledb_sys2::datatype::{Datatype, DatatypeError};
use tiledb_sys2::enumeration;
use tiledb_sys2::types::PhysicalType;

use crate::context::Context;
use crate::error::TileDBError;

pub struct Enumeration {
    pub(crate) enmr: cxx::SharedPtr<enumeration::Enumeration>,
}

impl Enumeration {
    pub(crate) fn new(enmr: cxx::SharedPtr<enumeration::Enumeration>) -> Self {
        Self { enmr }
    }

    pub fn name(&self) -> Result<String, TileDBError> {
        Ok(self.enmr.name()?)
    }

    pub fn datatype(&self) -> Result<Datatype, TileDBError> {
        Ok(self.enmr.datatype()?.try_into()?)
    }

    pub fn cell_val_num(&self) -> Result<u32, TileDBError> {
        Ok(self.enmr.cell_val_num()?)
    }

    pub fn ordered(&self) -> Result<bool, TileDBError> {
        Ok(self.enmr.ordered()?)
    }

    pub fn get_data(&self) -> Result<Buffer, TileDBError> {
        let mut buf = Buffer::new(self.datatype()?);
        self.enmr.get_data(Pin::new(&mut buf))?;
        Ok(buf)
    }

    pub fn get_offsets(&self) -> Result<Buffer, TileDBError> {
        let mut buf = Buffer::new(Datatype::UInt64);
        self.enmr.get_offsets(Pin::new(&mut buf))?;
        Ok(buf)
    }

    pub fn get_value_index<T: PhysicalType>(
        &self,
        value: T,
    ) -> Result<Option<u64>, TileDBError> {
        let mut buf = Buffer::from_vec(self.datatype()?, vec![value])?;
        self.get_index(&mut buf)
    }

    pub fn get_str_value_index(
        &self,
        value: &str,
    ) -> Result<Option<u64>, TileDBError> {
        let bytes = value
            .to_string()
            .into_bytes()
            .into_iter()
            .collect::<Vec<_>>();
        let mut buf = Buffer::from_vec(Datatype::StringUtf8, bytes)?;
        self.get_index(&mut buf)
    }

    pub fn get_index(
        &self,
        buf: &mut Buffer,
    ) -> Result<Option<u64>, TileDBError> {
        let mut idx = u64::MAX;
        let exists = self.enmr.get_index(Pin::new(buf), &mut idx)?;
        if exists { Ok(Some(idx)) } else { Ok(None) }
    }

    pub fn as_strings(&self) -> Result<Vec<String>, TileDBError> {
        let dtype = self.datatype()?;
        if !matches!(dtype, Datatype::StringAscii | Datatype::StringUtf8) {
            Err(DatatypeError::LogicalTypeMismatch {
                source_type: dtype,
                target_type: Datatype::StringUtf8,
            })?;
        }

        let data = self.get_data()?.into_vec::<u8>()?;
        let offsets = self.get_offsets()?.into_vec::<u64>()?;

        let mut ret = Vec::with_capacity(offsets.len());

        if offsets.is_empty() {
            return Ok(ret);
        }

        for idx in 1..offsets.len() {
            let val = String::from_utf8(
                data[offsets[idx - 1] as usize..offsets[idx] as usize].to_vec(),
            )?;
            ret.push(val);
        }

        let idx = *offsets.last().unwrap() as usize;
        let last = String::from_utf8(data[idx..].to_vec())?;
        ret.push(last);

        Ok(ret)
    }
}

pub struct EnumerationBuilder {
    ctx: Context,
    name: String,
    dtype: Datatype,
    cell_val_num: u32,
    ordered: bool,
    data: Buffer,
    offsets: Buffer,
}

impl EnumerationBuilder {
    pub fn new(
        ctx: &Context,
        name: &str,
        datatype: Datatype,
    ) -> Result<Self, TileDBError> {
        Ok(Self {
            ctx: ctx.clone(),
            name: name.to_string(),
            dtype: datatype,
            cell_val_num: 1,
            ordered: false,
            data: Buffer::new(datatype),
            offsets: Buffer::new(Datatype::UInt64),
        })
    }

    pub fn build(mut self) -> Result<Enumeration, TileDBError> {
        let enmr = enumeration::create_enumeration(
            self.ctx.ctx,
            &self.name,
            self.dtype.into(),
            self.cell_val_num,
            self.ordered,
            Pin::new(&mut self.data),
            Pin::new(&mut self.offsets),
        )?;
        Ok(Enumeration::new(enmr))
    }

    pub fn with_cell_val_num(mut self, cvn: u32) -> Result<Self, TileDBError> {
        self.cell_val_num = cvn;
        Ok(self)
    }

    pub fn with_ordered(mut self, ordered: bool) -> Result<Self, TileDBError> {
        self.ordered = ordered;
        Ok(self)
    }

    pub fn with_data<T: PhysicalType>(
        mut self,
        data: Vec<T>,
    ) -> Result<Self, TileDBError> {
        self.data = Buffer::from_vec(self.dtype, data)?;
        Ok(self)
    }

    pub fn with_offsets(
        mut self,
        offsets: Vec<u64>,
    ) -> Result<Self, TileDBError> {
        self.offsets = Buffer::from_vec(Datatype::UInt64, offsets)?;
        Ok(self)
    }

    pub fn from_strings<S: AsRef<str>>(
        ctx: &Context,
        name: S,
        values: &[S],
    ) -> Result<Self, TileDBError> {
        let len = values.iter().map(|s| s.as_ref().len()).sum();
        let mut data: Vec<u8> = Vec::with_capacity(len);
        let mut offsets: Vec<u64> = Vec::with_capacity(values.len());
        let mut curr_offset: u64 = 0;
        for val in values {
            let bytes = val.as_ref().as_bytes();
            data.extend_from_slice(bytes);
            offsets.push(curr_offset);
            curr_offset += bytes.len() as u64;
        }

        let builder = Self {
            ctx: ctx.clone(),
            name: name.as_ref().to_string(),
            dtype: Datatype::StringUtf8,
            cell_val_num: u32::MAX,
            ordered: false,
            data: Buffer::from_vec(Datatype::StringUtf8, data)?,
            offsets: Buffer::from_vec(Datatype::UInt64, offsets)?,
        };

        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() -> Result<(), TileDBError> {
        let ctx = Context::new()?;

        let data = "fredwilmabarneybetty"
            .to_string()
            .into_bytes()
            .into_iter()
            .collect::<Vec<_>>();
        let offsets = vec![0u64, 4, 9, 15];

        let enmr = EnumerationBuilder::new(
            &ctx,
            "flintstones",
            Datatype::StringAscii,
        )?
        .with_cell_val_num(u32::MAX)?
        .with_data(data)?
        .with_offsets(offsets)?
        .build()?;

        assert_eq!(enmr.name()?, "flintstones");
        assert_eq!(enmr.datatype()?, Datatype::StringAscii);
        assert_eq!(enmr.cell_val_num()?, u32::MAX);
        assert_eq!(enmr.ordered()?, false);
        assert!(enmr.get_data().is_ok());
        assert!(enmr.get_data().is_ok());

        let values = enmr.as_strings()?;
        let expect = vec![
            "fred".to_string(),
            "wilma".into(),
            "barney".into(),
            "betty".into(),
        ];
        assert_eq!(values, expect);

        assert_eq!(enmr.get_str_value_index("barney")?, Some(2));
        assert_eq!(enmr.get_str_value_index("bam bam")?, None);

        Ok(())
    }

    #[test]
    fn from_strings() -> Result<(), TileDBError> {
        let ctx = Context::new()?;

        let strings = vec!["carl", "doughnut", "mordecai", "katia"];

        let enmr =
            EnumerationBuilder::from_strings(&ctx, "crawlers", &strings[..])?
                .build()?;

        assert_eq!(enmr.name()?, "crawlers");
        assert_eq!(enmr.datatype()?, Datatype::StringUtf8);
        assert_eq!(enmr.cell_val_num()?, u32::MAX);
        assert_eq!(enmr.ordered()?, false);

        let data = enmr.get_data()?.into_vec::<u8>()?;
        let expect = "carldoughnutmordecaikatia"
            .to_string()
            .into_bytes()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(data, expect);

        let offsets = enmr.get_offsets()?.into_vec::<u64>()?;
        let expect = vec![0u64, 4, 12, 20];
        assert_eq!(offsets, expect);

        Ok(())
    }
}
