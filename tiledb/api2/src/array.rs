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
            array: array::create_array(ctx.ctx.clone(), uri)?,
        })
    }

    pub fn uri(&self) -> Result<String, TileDBError> {
        Ok(self.array.uri()?)
    }

    pub fn set_config(&mut self, cfg: &Config) -> Result<(), TileDBError> {
        self.array.set_config(cfg.cfg.clone())?;
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

    pub fn load_enumerations_all_schemas(&mut self) -> Result<(), TileDBError> {
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

    // Static methods for array operation

    pub fn create(
        ctx: &Context,
        uri: &str,
        schema: &Schema,
    ) -> Result<(), TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        array_ctx.create(schema.schema.clone())?;
        Ok(())
    }

    pub fn destroy(ctx: &Context, uri: &str) -> Result<(), TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        array_ctx.destroy()?;
        Ok(())
    }

    pub fn consolidate(ctx: &Context, uri: &str) -> Result<(), TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        array_ctx.consolidate()?;
        Ok(())
    }

    pub fn consolidate_with_config(
        ctx: &Context,
        uri: &str,
        cfg: &Config,
    ) -> Result<(), TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        array_ctx.consolidate_with_config(cfg.cfg.clone())?;
        Ok(())
    }

    pub fn consolidate_list(
        ctx: &Context,
        uri: &str,
        fragment_uris: &[&str],
    ) -> Result<(), TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        array_ctx.consolidate_list(fragment_uris)?;
        Ok(())
    }

    pub fn consolidate_list_with_config(
        ctx: &Context,
        uri: &str,
        fragment_uris: &[&str],
        cfg: &Config,
    ) -> Result<(), TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        array_ctx
            .consolidate_list_with_config(fragment_uris, cfg.cfg.clone())?;
        Ok(())
    }

    pub fn consolidate_metadata(
        ctx: &Context,
        uri: &str,
    ) -> Result<(), TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        array_ctx.consolidate_metadata()?;
        Ok(())
    }

    pub fn consolidate_metadata_with_config(
        ctx: &Context,
        uri: &str,
        cfg: &Config,
    ) -> Result<(), TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        array_ctx.consolidate_metadata_with_config(cfg.cfg.clone())?;
        Ok(())
    }

    pub fn delete_fragments(
        ctx: &Context,
        uri: &str,
        timestamp_start: u64,
        timestamp_end: u64,
    ) -> Result<(), TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        array_ctx.delete_fragments(timestamp_start, timestamp_end)?;
        Ok(())
    }

    pub fn delete_fragments_list(
        ctx: &Context,
        uri: &str,
        fragment_uris: &[&str],
    ) -> Result<(), TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        array_ctx.delete_fragments_list(fragment_uris)?;
        Ok(())
    }

    pub fn vacuum(ctx: &Context, uri: &str) -> Result<(), TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        array_ctx.vacuum()?;
        Ok(())
    }

    pub fn vacuum_with_config(
        ctx: &Context,
        uri: &str,
        cfg: &Config,
    ) -> Result<(), TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        array_ctx.vacuum_with_config(cfg.cfg.clone())?;
        Ok(())
    }

    pub fn load_schema(
        ctx: &Context,
        uri: &str,
    ) -> Result<Schema, TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        let schema = array_ctx.load_schema()?;
        Ok(Schema::new(schema))
    }

    pub fn load_schema_with_config(
        ctx: &Context,
        uri: &str,
        cfg: &Config,
    ) -> Result<Schema, TileDBError> {
        let array_ctx = array::create_array_context(ctx.ctx.clone(), uri)?;
        let schema = array_ctx.load_schema_with_config(cfg.cfg.clone())?;
        Ok(Schema::new(schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tiledb_common::array::ArrayType;
    use uri::TestArrayUri;

    #[test]
    fn create_array() -> Result<(), TileDBError> {
        let tmpuri = uri::get_uri_generator().unwrap();

        let ctx = Context::new()?;
        let uri = tmpuri.with_path("quickstart_dense").unwrap();
        let schema =
            crate::tests::create_quickstart_schema(&ctx, ArrayType::Dense)?;
        Array::create(&ctx, &uri, &schema)?;

        let uri = tmpuri.with_path("quickstart_sparse").unwrap();
        let schema =
            crate::tests::create_quickstart_schema(&ctx, ArrayType::Sparse)?;
        Array::create(&ctx, &uri, &schema)?;

        Ok(())
    }

    #[test]
    fn check_array_methods() -> Result<(), TileDBError> {
        let tmpuri = uri::get_uri_generator().unwrap();

        let ctx = Context::new()?;
        let uri = tmpuri.with_path("quickstart").unwrap();
        let schema =
            crate::tests::create_quickstart_schema(&ctx, ArrayType::Sparse)?;
        Array::create(&ctx, &uri, &schema)?;

        let mut array = Array::new(&ctx, &uri)?;
        let cfg = Config::new()?;

        assert!(array.uri()?.ends_with("quickstart"));
        assert!(array.set_config(&cfg).is_ok());
        assert!(array.set_open_timestamp_start(1).is_ok());
        assert!(array.set_open_timestamp_end(10).is_ok());
        assert!(array.open(Mode::Read).is_ok());
        assert!(array.reopen().is_ok());
        assert!(array.is_open()?);
        assert_eq!(array.mode()?, Mode::Read);
        assert!(array.config().is_ok());
        assert!(array.schema().is_ok());
        assert_eq!(array.open_timestamp_start()?, 1);

        // The open_timestamp_end gets adjusted based on the timestamp of
        // the schema creation. We only care that it doesn't throw an
        // exception.
        assert!(array.open_timestamp_end()? > 10);

        let err = array.get_enumeration("foo").err().unwrap();
        let err = format!("{err}");
        assert!(err.contains("No enumeration named 'foo'"));

        assert!(array.load_all_enumerations().is_ok());
        assert!(array.load_enumerations_all_schemas().is_ok());

        assert!(array.non_empty_domain_from_index(0).is_ok());
        assert!(array.non_empty_domain_from_name("rows").is_ok());
        assert!(array.non_empty_domain_var_from_index(0).is_err());
        assert!(array.non_empty_domain_var_from_name("rows").is_err());

        let err = array.put_metadata("foo", Datatype::Int32, 1, &[1i32]);
        let err = format!("{}", err.err().unwrap());
        assert!(err.contains(
            "Array was not opened in write or modify_exclusive mode"
        ));

        let err = array.get_metadata("foo").err().unwrap();
        let err = format!("{err}");
        assert!(err.contains("Metadata key 'foo' was not found."));

        let err = array.delete_metadata("foo").err().unwrap();
        let err = format!("{err}");
        assert!(err.contains(
            "Array was not opened in write or modify_exclusive mode"
        ));

        assert_eq!(array.num_metadata()?, 0);

        let err = array.get_metadata_from_index(0).err().unwrap();
        let err = format!("{err}");
        assert!(err.contains("Cannot get metadata; index out of bounds"));

        assert!(array.close().is_ok());
        assert!(!array.is_open()?);

        let cfg = Config::new()?;

        assert!(Array::consolidate(&ctx, &uri).is_ok());
        assert!(Array::consolidate_with_config(&ctx, &uri, &cfg).is_ok());
        assert!(Array::consolidate_list(&ctx, &uri, &[]).is_ok());
        assert!(
            Array::consolidate_list_with_config(&ctx, &uri, &[], &cfg).is_ok()
        );
        assert!(Array::consolidate_metadata(&ctx, &uri).is_ok());
        assert!(
            Array::consolidate_metadata_with_config(&ctx, &uri, &cfg).is_ok()
        );
        assert!(Array::delete_fragments(&ctx, &uri, 0, 10).is_ok());

        let err = Array::delete_fragments_list(&ctx, &uri, &[]).err().unwrap();
        let err = format!("{err}");
        assert!(err.contains("Invalid input number of fragments"));

        assert!(Array::vacuum(&ctx, &uri).is_ok());
        assert!(Array::vacuum_with_config(&ctx, &uri, &cfg).is_ok());

        let schema = Array::load_schema(&ctx, &uri)?;
        assert!(schema.attribute_from_name("a").is_ok());

        let schema = Array::load_schema_with_config(&ctx, &uri, &cfg)?;
        assert!(schema.domain()?.dimension_from_name("rows").is_ok());

        Ok(())
    }
}

// TODO: Tests. But racing towards query right this moment.
