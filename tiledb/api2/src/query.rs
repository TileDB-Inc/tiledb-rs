//! Query docs go here.
//!
//! ToDo: I need to separate the query types out into specific read/write types
//! because some functions only apply to queries in read or write mode. So far:
//!
//! Write only:
//!   * num_fragments
//!   * fragment_uri
//!   * fragment_timestamp_range
//!
//! Read only:
//!   * Pretty sure num_relevant_fragments even though it returns 0 on writes

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

    /// Get the estimated buffer size for a field.
    ///
    /// The returned size may not be large enough to contain all query results.
    /// The query status should be used to know if a query has completed or
    /// if it needs to be resubmitted.
    ///
    /// Result values are returned as the number of elements of the buffer
    /// which is different than the number of bytes required. That would be
    /// elements * dtype.size() to get the bytes required.
    pub fn est_result_size(
        &mut self,
        field: &str,
    ) -> Result<(usize, Option<usize>, Option<usize>), TileDBError> {
        let (dtype, var, nullable) = self.array.schema()?.field_info(field)?;

        let mut data_size = 0;
        let mut offsets_size = 0;
        let mut validity_size = 0;

        if !var && !nullable {
            self.query.est_result_size(field, &mut data_size)?;
            assert!(
                data_size as usize % dtype.size() == 0,
                "Invalid data buffer."
            );
            Ok((data_size as usize / dtype.size(), None, None))
        } else if var && !nullable {
            self.query.est_result_size_var(
                field,
                &mut data_size,
                &mut offsets_size,
            )?;
            assert!(
                data_size as usize % dtype.size() == 0,
                "Invalid data buffer."
            );
            assert!(offsets_size % 8 == 0, "Inlvaid offsets buffer.");
            Ok((
                data_size as usize / dtype.size(),
                Some(offsets_size as usize / 8),
                None,
            ))
        } else if !var && nullable {
            self.query.est_result_size_nullable(
                field,
                &mut data_size,
                &mut validity_size,
            )?;
            assert!(
                data_size as usize % dtype.size() == 0,
                "Invalid data buffer."
            );
            Ok((
                data_size as usize / dtype.size(),
                None,
                Some(validity_size as usize),
            ))
        } else {
            self.query.est_result_size_var_nullable(
                field,
                &mut data_size,
                &mut offsets_size,
                &mut validity_size,
            )?;
            assert!(
                data_size as usize % dtype.size() == 0,
                "Invalid data buffer."
            );
            assert!(offsets_size % 8 == 0, "Invalid offsets buffer");
            Ok((
                data_size as usize / dtype.size(),
                Some(offsets_size as usize / 8),
                Some(validity_size as usize),
            ))
        }
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
        let (dtype, var, nullable) = schema.field_info(field)?;

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

    use tiledb_common::array::{ArrayType, TileOrder};
    use tiledb_sys2::datatype::Datatype;
    use uri::TestArrayUri;

    use crate::attribute::AttributeBuilder;
    use crate::dimension::DimensionBuilder;
    use crate::domain::DomainBuilder;
    use crate::schema::SchemaBuilder;

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
        assert!(query.config().is_ok());
        assert_eq!(query.layout()?, CellOrder::RowMajor);

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

        let b = query.get_offsets_slice("b");
        assert!(b.is_err());
        let msg = format!("{}", b.err().unwrap());
        assert_eq!(msg, "The field 'b' was not found.");

        let b = query.get_validity_slice("b");
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

    #[test]
    fn check_all_field_types() -> Result<(), TileDBError> {
        let ctx = Context::new()?;

        let id = DimensionBuilder::new(&ctx, "id", Datatype::Int32)?
            .with_domain(&[0, 4])?
            .with_tile_extent(1)?
            .build()?;

        let dom = DomainBuilder::new(&ctx)?.with_dimension(id)?.build()?;

        let a = AttributeBuilder::new(&ctx, "a", Datatype::Int32)?.build()?;
        let b = AttributeBuilder::new(&ctx, "b", Datatype::Int32)?
            .with_nullable(true)?
            .build()?;
        let c = AttributeBuilder::new(&ctx, "c", Datatype::StringUtf8)?
            .with_cell_val_num(u32::MAX)?
            .build()?;
        let d = AttributeBuilder::new(&ctx, "d", Datatype::StringUtf8)?
            .with_cell_val_num(u32::MAX)?
            .with_nullable(true)?
            .build()?;

        let schema = SchemaBuilder::new(&ctx, ArrayType::Sparse)?
            .with_capacity(1000)?
            .with_tile_order(TileOrder::RowMajor)?
            .with_cell_order(CellOrder::RowMajor)?
            .with_domain(dom)?
            .with_attributes(&[a, b, c, d])?
            .build()?;

        let tmpuri = uri::get_uri_generator().unwrap();
        let uri = tmpuri.with_path("quickstart_dense").unwrap();

        Array::create(&ctx, &uri, &schema)?;

        let mut array = Array::new(&ctx, &uri)?;
        array.open(Mode::Write)?;

        let id_data = vec![0i32, 1, 2, 3];
        let a_data = vec![100i32, 200, 300, 400];
        let b_data = vec![0i32, 1, 0, 1];
        let b_validity = vec![0u8, 1, 0, 1];
        let c_data = "carldoughnutkatiamordecai".to_owned().into_bytes();
        let c_offsets = vec![0u64, 4, 12, 16];
        let d_data = "jasonhumphreyclivesophie".to_owned().into_bytes();
        let d_offsets = vec![0u64, 5, 13, 18];
        let d_validity = vec![1u8, 1, 1, 1];

        let id_field = QueryBuffers::try_from((Datatype::Int32, id_data))?;

        let fields = vec![
            ("a", QueryBuffers::try_from((Datatype::Int32, a_data))?),
            (
                "b",
                QueryBuffers::try_from((Datatype::Int32, b_data, b_validity))?,
            ),
            (
                "c",
                QueryBuffers::try_from((
                    Datatype::StringUtf8,
                    c_data,
                    c_offsets,
                ))?,
            ),
            (
                "d",
                QueryBuffers::try_from((
                    Datatype::StringUtf8,
                    d_data,
                    d_offsets,
                    d_validity,
                ))?,
            ),
        ];

        let mut query = QueryBuilder::new(&ctx, array, Mode::Write)?
            .with_layout(CellOrder::Unordered)?
            .with_field("id", id_field)?
            .with_fields(fields)?
            .build()?;

        query.submit()?;
        assert_eq!(query.status()?, QueryStatus::Completed);
        assert!(!query.has_results()?);

        assert_eq!(query.num_fragments()?, 1);
        assert!(!query.fragment_uri(0)?.is_empty());

        assert_eq!(query.num_relevant_fragments()?, 0);

        let range = query.fragment_timestamp_range(0)?;
        assert!(range.0 > 0 && range.1 >= range.0);

        let (_, fields) = query.finalize()?;

        let mut array = Array::new(&ctx, &uri)?;
        array.open(Mode::Read)?;

        let mut query = QueryBuilder::new(&ctx, array, Mode::Read)?
            .with_layout(CellOrder::RowMajor)?
            .with_allocated_field("id", 128)?
            .with_allocated_fields(&["a", "b", "c", "d"], 128)?
            .build()?;

        let sizes = query.est_result_size("id")?;
        assert!(sizes.0 > 0 && sizes.1.is_none() && sizes.2.is_none());

        let sizes = query.est_result_size("a")?;
        assert!(sizes.0 > 0 && sizes.1.is_none() && sizes.2.is_none());

        let sizes = query.est_result_size("b")?;
        assert!(sizes.0 > 0 && sizes.1.is_none() && sizes.2.is_some());

        let sizes = query.est_result_size("c")?;
        assert!(sizes.0 > 0 && sizes.1.is_some() && sizes.2.is_none());

        let sizes = query.est_result_size("d")?;
        assert!(sizes.0 > 0 && sizes.1.is_some() && sizes.2.is_some());

        query.submit()?;
        assert_eq!(query.status()?, QueryStatus::Completed);

        let id_result = query.get_data_slice::<i32>("id")?;
        let id_data = fields.get("id").unwrap().data.as_slice::<i32>()?;
        assert_eq!(id_result, id_data);

        let a_result = query.get_data_slice::<i32>("a")?;
        let a_data = fields.get("a").unwrap().data.as_slice::<i32>()?;
        assert_eq!(a_result, a_data);

        let b_result = query.get_data_slice::<i32>("b")?;
        let b_data = fields.get("b").unwrap().data.as_slice::<i32>()?;
        assert_eq!(b_result, b_data);

        let b_result_validity = query.get_validity_slice("b")?;
        let b_validity = fields
            .get("b")
            .unwrap()
            .validity
            .as_ref()
            .unwrap()
            .as_slice::<u8>()?;
        assert_eq!(b_result_validity, b_validity);

        let c_result = query.get_data_slice::<u8>("c")?;
        let c_data = fields.get("c").unwrap().data.as_slice::<u8>()?;
        assert_eq!(c_result, c_data);

        let c_result_offsets = query.get_offsets_slice("c")?;
        let c_offsets = fields
            .get("c")
            .unwrap()
            .offsets
            .as_ref()
            .unwrap()
            .as_slice::<u64>()?;
        assert_eq!(c_result_offsets, c_offsets);

        let d_result = query.get_data_slice::<u8>("d")?;
        let d_data = fields.get("d").unwrap().data.as_slice::<u8>()?;
        assert_eq!(d_result, d_data);

        let d_result_offsets = query.get_offsets_slice("d")?;
        let d_offsets = fields
            .get("d")
            .unwrap()
            .offsets
            .as_ref()
            .unwrap()
            .as_slice::<u64>()?;
        assert_eq!(d_result_offsets, d_offsets);

        let d_result_validity = query.get_validity_slice("d")?;
        let d_validity = fields
            .get("d")
            .unwrap()
            .validity
            .as_ref()
            .unwrap()
            .as_slice::<u8>()?;
        assert_eq!(d_result_validity, d_validity);

        assert_eq!(query.num_relevant_fragments()?, 1);

        assert!(query.stats().is_ok());

        Ok(())
    }

    #[test]
    fn check_submit_and_finalize() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        let tmpuri = uri::get_uri_generator().unwrap();
        let uri = tmpuri.with_path("quickstart_dense").unwrap();

        crate::tests::create_quickstart_array(&ctx, &uri, ArrayType::Sparse)?;

        let fields: Vec<(&str, QueryBuffers)> = vec![
            ("rows", (Datatype::Int32, vec![1i32, 2, 2]).try_into()?),
            ("columns", (Datatype::Int32, vec![1i32, 4, 3]).try_into()?),
            ("a", (Datatype::Int32, vec![1i32, 2, 3]).try_into()?),
        ];

        let mut array = Array::new(&ctx, &uri)?;
        array.open(Mode::Write)?;

        let cfg = Config::new()?;
        let mut query = QueryBuilder::new(&ctx, array, Mode::Write)?
            .with_config(cfg)?
            .with_layout(CellOrder::Unordered)?
            .with_fields(fields)?
            .build()?;

        assert_eq!(query.mode()?, Mode::Write);
        let err = query.submit_and_finalize();
        assert!(err.is_err());
        let msg = format!("{}", err.err().unwrap());
        assert!(msg.contains("Call valid only in global_order writes."));

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

        let cfg = Config::new()?;
        let mut query = QueryBuilder::new(ctx, array, Mode::Write)?
            .with_config(cfg)?
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
