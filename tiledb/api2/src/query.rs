use std::collections::HashMap;
use std::pin::Pin;

use tiledb_common::array::{CellOrder, Mode};
use tiledb_common::query::QueryStatus;
use tiledb_sys2::query;
use tiledb_sys2::types::PhysicalType;

use crate::array::Array;
use crate::config::Config;
use crate::context::Context;
use crate::error::TileDBError;
use crate::query_buffers::QueryBuffers;

pub struct Query {
    pub(crate) query: cxx::SharedPtr<query::Query>,
    pub(crate) array: Array,
    pub(crate) buffers: HashMap<String, QueryBuffers>,
}

impl Query {
    pub(crate) fn new(
        query: cxx::SharedPtr<query::Query>,
        array: Array,
        buffers: HashMap<String, QueryBuffers>,
    ) -> Result<Self, TileDBError> {
        Ok(Self {
            query,
            array,
            buffers,
        })
    }

    pub fn mode(&mut self) -> Result<Mode, TileDBError> {
        Ok(self.query.mode()?.try_into()?)
    }

    pub fn config(&mut self) -> Result<Config, TileDBError> {
        let cfg = self.query.config()?;
        Ok(Config::from_ptr(cfg))
    }

    pub fn layout(&mut self) -> Result<CellOrder, TileDBError> {
        Ok(self.query.layout()?.try_into()?)
    }

    pub fn submit(&mut self) -> Result<(), TileDBError> {
        for (field, buffers) in self.buffers.iter_mut() {
            self.query
                .set_data_buffer(field.as_str(), Pin::new(&mut buffers.data))?;

            if let Some(ref mut offsets) = buffers.offsets {
                self.query
                    .set_offsets_buffer(field.as_str(), Pin::new(offsets))?;
            }

            if let Some(ref mut validity) = buffers.validity {
                self.query
                    .set_validity_buffer(field.as_str(), Pin::new(validity))?;
            }
        }

        Ok(self.query.submit()?)
    }

    pub fn finalize(
        self,
    ) -> Result<(Array, HashMap<String, QueryBuffers>), TileDBError> {
        self.query.finalize()?;
        Ok((self.array, self.buffers))
    }

    pub fn submit_and_finalize(
        self,
    ) -> Result<(Array, HashMap<String, QueryBuffers>), TileDBError> {
        self.query.submit_and_finalize()?;
        Ok((self.array, self.buffers))
    }

    pub fn status(&mut self) -> Result<QueryStatus, TileDBError> {
        Ok(self.query.status()?.try_into()?)
    }

    pub fn has_results(&mut self) -> Result<bool, TileDBError> {
        Ok(self.query.has_results()?)
    }

    pub fn get_data_slice<T: PhysicalType>(
        &mut self,
        field: &str,
    ) -> Result<&[T], TileDBError> {
        let Some(sizes) = self.get_buffer_sizes(field)? else {
            return Err(TileDBError::UnknownField(field.into()));
        };

        if let Some(qb) = self.buffers.get_mut(field) {
            qb.data.resize_bytes(sizes.0 as usize);
            Ok(qb.data.as_slice::<T>()?)
        } else {
            Err(TileDBError::UnknownField(field.into()))
        }
    }

    pub fn get_offsets_slice(
        &mut self,
        field: &str,
    ) -> Result<&[u64], TileDBError> {
        let Some(sizes) = self.get_buffer_sizes(field)? else {
            return Err(TileDBError::UnknownField(field.into()));
        };

        if let Some(qb) = self.buffers.get_mut(field) {
            if let Some(offsets) = qb.offsets.as_mut() {
                offsets.resize_bytes(sizes.1 as usize);
                Ok(offsets.as_slice::<u64>()?)
            } else {
                Err(TileDBError::NonVariable(field.into()))
            }
        } else {
            Err(TileDBError::UnknownField(field.into()))
        }
    }

    pub fn get_validity_slice(
        &mut self,
        field: &str,
    ) -> Result<&[u8], TileDBError> {
        let Some(sizes) = self.get_buffer_sizes(field)? else {
            return Err(TileDBError::UnknownField(field.into()));
        };

        if let Some(qb) = self.buffers.get_mut(field) {
            if let Some(validity) = qb.validity.as_mut() {
                validity.resize_bytes(sizes.2 as usize);
                Ok(validity.as_slice::<u8>()?)
            } else {
                Err(TileDBError::NonNullable(field.into()))
            }
        } else {
            Err(TileDBError::UnknownField(field.into()))
        }
    }

    pub fn est_result_size(
        &mut self,
        name: &str,
    ) -> Result<(u64, u64, u64), TileDBError> {
        let mut data_size = 0;
        let mut offsets_size = 0;
        let mut validity_size = 0;
        self.query.est_result_size(
            name,
            &mut data_size,
            &mut offsets_size,
            &mut validity_size,
        )?;
        Ok((data_size, offsets_size, validity_size))
    }

    pub fn num_fragments(&mut self) -> Result<u32, TileDBError> {
        Ok(self.query.num_fragments()?)
    }

    pub fn num_relevant_fragments(&mut self) -> Result<u64, TileDBError> {
        Ok(self.query.num_relevant_fragments()?)
    }

    pub fn fragment_uri(&mut self, index: u32) -> Result<String, TileDBError> {
        Ok(self.query.fragment_uri(index)?)
    }

    pub fn fragment_timestamp_range(
        &mut self,
        index: u32,
    ) -> Result<(u64, u64), TileDBError> {
        let mut lo = 0;
        let mut hi = 0;
        self.query
            .fragment_timestamp_range(index, &mut lo, &mut hi)?;
        Ok((lo, hi))
    }

    pub fn stats(&mut self) -> Result<String, TileDBError> {
        Ok(self.query.stats()?)
    }

    fn get_buffer_sizes(
        &mut self,
        field: &str,
    ) -> Result<Option<(u64, u64, u64)>, TileDBError> {
        let mut data_size = 0u64;
        let mut offsets_size = 0u64;
        let mut validity_size = 0u64;
        let found = self.query.get_buffer_sizes(
            field,
            &mut data_size,
            &mut offsets_size,
            &mut validity_size,
        )?;

        if !found {
            return Ok(None);
        }

        Ok(Some((data_size, offsets_size, validity_size)))
    }
}

pub struct QueryBuilder {
    builder: cxx::SharedPtr<query::QueryBuilder>,
    array: Array,
    buffers: HashMap<String, QueryBuffers>,
}

impl QueryBuilder {
    pub fn new(
        ctx: &Context,
        array: Array,
        mode: Mode,
    ) -> Result<Self, TileDBError> {
        Ok(Self {
            builder: query::create_query_builder(
                ctx.ctx.clone(),
                array.array.clone(),
                mode.into(),
            )?,
            array,
            buffers: Default::default(),
        })
    }

    pub fn build(self) -> Result<Query, TileDBError> {
        Query::new(self.builder.build()?, self.array, self.buffers)
    }

    pub fn with_layout(self, order: CellOrder) -> Result<Self, TileDBError> {
        self.builder.set_layout(order.into())?;
        Ok(self)
    }

    pub fn with_config(self, config: Config) -> Result<Self, TileDBError> {
        self.builder.set_config(config.cfg.clone())?;
        Ok(self)
    }

    pub fn with_field(
        mut self,
        name: &str,
        buffers: QueryBuffers,
    ) -> Result<Self, TileDBError> {
        self.buffers.insert(name.into(), buffers);
        Ok(self)
    }

    pub fn with_fields(
        mut self,
        fields: Vec<(&str, QueryBuffers)>,
    ) -> Result<Self, TileDBError> {
        for (field, buffers) in fields.into_iter() {
            self.buffers.insert(field.to_string(), buffers);
        }
        Ok(self)
    }

    pub fn with_allocated_fields<F: AsRef<str>>(
        mut self,
        fields: &[F],
        elements: usize,
    ) -> Result<Self, TileDBError> {
        for field in fields {
            self = self.with_allocated_field_impl(field.as_ref(), elements)?;
        }

        Ok(self)
    }

    pub fn with_allocated_field<F: AsRef<str>>(
        self,
        field: F,
        elements: usize,
    ) -> Result<Self, TileDBError> {
        self.with_allocated_field_impl(field.as_ref(), elements)
    }

    fn with_allocated_field_impl(
        mut self,
        field: &str,
        elements: usize,
    ) -> Result<Self, TileDBError> {
        if elements == 0 {
            return Err(TileDBError::InvalidCapacity);
        }

        let schema = self.array.schema()?;
        let (dtype, var, nullable) = if schema.has_attribute(field)? {
            let attr = schema.attribute_from_name(field)?;
            let dtype = attr.datatype()?;
            let var = attr.cell_val_num()? == u32::MAX;
            let nullable = attr.nullable()?;
            (dtype, var, nullable)
        } else if schema.domain()?.has_dimension(field)? {
            let dim = schema.domain()?.dimension_from_name(field)?;
            let dtype = dim.datatype()?;
            let var = dim.cell_val_num()? == u32::MAX;
            (dtype, var, false)
        } else {
            return Err(TileDBError::UnknownField(field.into()));
        };

        let mut buffers = QueryBuffers::with_capacity(dtype, elements);
        buffers.data.resize(elements);

        let buffers = if var {
            let mut buffers = buffers.with_offsets(elements);
            buffers.offsets.as_mut().unwrap().resize(elements);
            buffers
        } else {
            buffers
        };

        let buffers = if nullable {
            let mut buffers = buffers.with_validity(elements);
            buffers.validity.as_mut().unwrap().resize(elements);
            buffers
        } else {
            buffers
        };

        self.buffers.insert(field.into(), buffers);

        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tiledb_common::array::ArrayType;
    use tiledb_sys2::datatype::Datatype;
    use uri::TestArrayUri;

    #[test]
    fn write_data() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        let tmpuri = uri::get_uri_generator().unwrap();
        let uri = tmpuri.with_path("quickstart_dense").unwrap();

        write_data_impl(&ctx, &uri)
    }

    #[test]
    fn read_data() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        let tmpuri = uri::get_uri_generator().unwrap();
        let uri = tmpuri.with_path("quickstart_dense").unwrap();

        write_data_impl(&ctx, &uri)?;

        let mut array = Array::new(&ctx, &uri)?;
        array.open(Mode::Read)?;

        let mut query = QueryBuilder::new(&ctx, array, Mode::Read)?
            .with_layout(CellOrder::RowMajor)?
            .with_allocated_fields(&["rows", "columns", "a"], 1024)?
            .build()?;

        assert_eq!(query.mode()?, Mode::Read);

        query.submit()?;
        assert_eq!(query.status()?, QueryStatus::Completed);
        assert!(query.has_results()?);

        let rows = query.get_data_slice::<i32>("rows")?;
        assert_eq!(rows, vec![1i32, 2, 2]);

        let columns = query.get_data_slice::<i32>("columns")?;
        assert_eq!(columns, vec![1i32, 3, 4]);

        let a = query.get_data_slice::<i32>("a")?;
        assert_eq!(a, vec![1i32, 3, 2]);

        // Check for missing field
        let b = query.get_data_slice::<i32>("b");
        assert!(b.is_err());
        let msg = format!("{}", b.err().unwrap());
        assert_eq!(msg, "The field 'b' was not found.");

        // Check for missing offsets and validity
        let a_offsets = query.get_offsets_slice("a");
        assert!(a_offsets.is_err());
        let msg = format!("{}", a_offsets.err().unwrap());
        assert_eq!(msg, "The field 'a' is not variably sized");

        let a_validity = query.get_validity_slice("a");
        assert!(a_validity.is_err());
        let msg = format!("{}", a_validity.err().unwrap());
        assert_eq!(msg, "The field 'a' is not nullable.");

        Ok(())
    }

    fn write_data_impl(ctx: &Context, uri: &str) -> Result<(), TileDBError> {
        crate::tests::create_quickstart_array(ctx, uri, ArrayType::Sparse)?;

        let fields: Vec<(&str, QueryBuffers)> = vec![
            ("rows", (Datatype::Int32, vec![1i32, 2, 2]).try_into()?),
            ("columns", (Datatype::Int32, vec![1i32, 4, 3]).try_into()?),
            ("a", (Datatype::Int32, vec![1i32, 2, 3]).try_into()?),
        ];

        let mut array = Array::new(ctx, uri)?;
        array.open(Mode::Write)?;

        let mut query = QueryBuilder::new(ctx, array, Mode::Write)?
            .with_layout(CellOrder::Unordered)?
            .with_fields(fields)?
            .build()?;

        assert_eq!(query.mode()?, Mode::Write);

        query.submit()?;
        assert_eq!(query.status()?, QueryStatus::Completed);
        assert!(!query.has_results()?);
        query.finalize()?;

        Ok(())
    }
}
