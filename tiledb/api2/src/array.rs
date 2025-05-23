use std::pin::Pin;

use tiledb_common::array::Mode;
use tiledb_sys2::array;
use tiledb_sys2::buffer::Buffer;
use tiledb_sys2::datatype::{Datatype, FFIDatatype};
use tiledb_sys2::types::PhysicalType;

use crate::config::Config;
use crate::context::Context;
use crate::enumeration::Enumeration;
use crate::error::TileDBError;
use crate::schema::Schema;

pub struct Array {
    pub(crate) array: cxx::SharedPtr<array::Array>,
}

impl Array {
    pub fn new(ctx: &Context, uri: &str) -> Result<Self, TileDBError> {
        Ok(Self {
            array: array::create_array(ctx.clone().ctx, uri)?,
        })
    }

    pub fn uri(&mut self) -> Result<String, TileDBError> {
        Ok(self.array.uri()?)
    }

    pub fn set_config(&mut self, cfg: Config) -> Result<(), TileDBError> {
        self.array.set_config(cfg.cfg)?;
        Ok(())
    }

    pub fn set_open_timestamp_start(
        &mut self,
        timestamp: u64,
    ) -> Result<(), TileDBError> {
        self.array.set_open_timestamp_start(timestamp)?;
        Ok(())
    }

    pub fn set_open_timestamp_end(
        &mut self,
        timestamp: u64,
    ) -> Result<(), TileDBError> {
        self.array.set_open_timestamp_end(timestamp)?;
        Ok(())
    }

    pub fn open(&mut self, mode: Mode) -> Result<(), TileDBError> {
        self.array.open(mode.into())?;
        Ok(())
    }

    pub fn reopen(&mut self) -> Result<(), TileDBError> {
        self.array.reopen()?;
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), TileDBError> {
        self.array.close()?;
        Ok(())
    }

    pub fn is_open(&mut self) -> Result<bool, TileDBError> {
        Ok(self.array.is_open()?)
    }

    pub fn mode(&mut self) -> Result<Mode, TileDBError> {
        Ok(self.array.mode()?.try_into()?)
    }

    pub fn config(&mut self) -> Result<Config, TileDBError> {
        let cfg = self.array.config()?;
        Ok(Config::from_ptr(cfg))
    }

    pub fn schema(&mut self) -> Result<Schema, TileDBError> {
        let schema = self.array.schema()?;
        Ok(Schema::new(schema))
    }

    pub fn open_timestamp_start(&mut self) -> Result<u64, TileDBError> {
        Ok(self.array.open_timestamp_start()?)
    }

    pub fn open_timestamp_end(&mut self) -> Result<u64, TileDBError> {
        Ok(self.array.open_timestamp_end()?)
    }

    pub fn get_enumeration(
        &mut self,
        attr_name: &str,
    ) -> Result<Enumeration, TileDBError> {
        let enmr = self.array.get_enumeration(attr_name)?;
        Ok(Enumeration::new(enmr))
    }

    pub fn load_all_enumerations(&mut self) -> Result<(), TileDBError> {
        self.array.load_all_enumerations()?;
        Ok(())
    }

    pub fn load_enumerations_all_scheams(&mut self) -> Result<(), TileDBError> {
        self.array.load_enumerations_all_schemas()?;
        Ok(())
    }

    pub fn non_empty_domain_from_index(
        &mut self,
        index: u32,
    ) -> Result<Buffer, TileDBError> {
        let dim = self.array.schema()?.domain()?.dimension_from_index(index)?;
        let mut buf = Buffer::new(dim.datatype()?.try_into()?);

        self.array
            .non_empty_domain_from_index(index, Pin::new(&mut buf))?;

        Ok(buf)
    }

    pub fn non_empty_domain_from_name(
        &mut self,
        name: &str,
    ) -> Result<Buffer, TileDBError> {
        let dim = self.array.schema()?.domain()?.dimension_from_name(name)?;
        let mut buf = Buffer::new(dim.datatype()?.try_into()?);

        self.array
            .non_empty_domain_from_name(name, Pin::new(&mut buf))?;

        Ok(buf)
    }

    // TODO: Probably coerce the returns to strings.
    pub fn non_empty_domain_var_from_index(
        &mut self,
        index: u32,
    ) -> Result<(Buffer, Buffer), TileDBError> {
        let dim = self.array.schema()?.domain()?.dimension_from_index(index)?;
        let mut lower = Buffer::new(dim.datatype()?.try_into()?);
        let mut upper = Buffer::new(dim.datatype()?.try_into()?);

        self.array.non_empty_domain_var_from_index(
            index,
            Pin::new(&mut lower),
            Pin::new(&mut upper),
        )?;

        Ok((lower, upper))
    }

    pub fn non_empty_domain_var_from_name(
        &mut self,
        name: &str,
    ) -> Result<(Buffer, Buffer), TileDBError> {
        let dim = self.array.schema()?.domain()?.dimension_from_name(name)?;
        let mut lower = Buffer::new(dim.datatype()?.try_into()?);
        let mut upper = Buffer::new(dim.datatype()?.try_into()?);

        self.array.non_empty_domain_var_from_name(
            name,
            Pin::new(&mut lower),
            Pin::new(&mut upper),
        )?;

        Ok((lower, upper))
    }

    // TODO: add put_metadata_str
    pub fn put_metadata<T: PhysicalType>(
        &mut self,
        name: &str,
        dtype: Datatype,
        num: u32,
        values: &[T],
    ) -> Result<(), TileDBError> {
        let mut buf = Buffer::from_vec(dtype, values.to_vec())?;

        self.array
            .put_metadata(name, dtype.into(), num, Pin::new(&mut buf))?;

        Ok(())
    }

    pub fn get_metadata(
        &mut self,
        name: &str,
    ) -> Result<(Datatype, Buffer), TileDBError> {
        let mut dtype = FFIDatatype::Any;
        let mut buf = Buffer::uninit();

        self.array
            .get_metadata(name, &mut dtype, Pin::new(&mut buf))?;

        Ok((dtype.try_into()?, buf))
    }

    pub fn delete_metadata(&mut self, name: &str) -> Result<(), TileDBError> {
        self.array.delete_metadata(name)?;
        Ok(())
    }

    pub fn num_metadata(&mut self) -> Result<u64, TileDBError> {
        Ok(self.array.num_metadata()?)
    }

    pub fn get_metadata_from_index(
        &mut self,
        index: u64,
    ) -> Result<(String, Datatype, Buffer), TileDBError> {
        let mut key = Vec::new();
        let mut dtype = FFIDatatype::Any;
        let mut buf = Buffer::uninit();

        self.array.get_metadata_from_index(
            index,
            &mut key,
            &mut dtype,
            Pin::new(&mut buf),
        )?;

        let key = String::from_utf8(key)?;
        Ok((key, dtype.try_into()?, buf))
    }
}

// TODO: Tests. But racing towards query right this moment.
