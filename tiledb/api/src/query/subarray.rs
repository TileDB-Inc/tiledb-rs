use std::marker::PhantomData;
use std::ops::Deref;

use anyhow::anyhow;
use itertools::Itertools;

use crate::array::Schema;
use crate::context::{CApiInterface, Context, ContextBound};
use crate::datatype::PhysicalType;
use crate::error::{DatatypeErrorKind, Error};
use crate::key::LookupKey;
use crate::query::QueryBuilder;
use crate::range::{Range, SingleValueRange, TypedRange, VarValueRange};
use crate::Result as TileDBResult;
use crate::{physical_type_go, single_value_range_go, var_value_range_go};

pub(crate) enum RawSubarray {
    Owned(*mut ffi::tiledb_subarray_t),
}

impl Deref for RawSubarray {
    type Target = *mut ffi::tiledb_subarray_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawSubarray::Owned(ref ffi) => ffi,
        }
    }
}

impl Drop for RawSubarray {
    fn drop(&mut self) {
        let RawSubarray::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_subarray_free(ffi) };
    }
}

pub struct Subarray<'query> {
    schema: Schema,
    raw: RawSubarray,
    _marker: PhantomData<&'query ()>,
}

impl<'query> ContextBound for Subarray<'query> {
    fn context(&self) -> Context {
        self.schema.context()
    }
}

impl<'query> Subarray<'query> {
    pub(crate) fn capi(&self) -> *mut ffi::tiledb_subarray_t {
        *self.raw
    }

    pub(crate) fn new(schema: Schema, raw: RawSubarray) -> Self {
        Subarray {
            schema,
            raw,
            _marker: Default::default(),
        }
    }

    /// Return all dimension ranges set on the query.
    /// The outer `Vec` is indexed by the dimension number,
    /// and the inner `Vec` is the set of ranges set for that dimension.
    pub fn ranges(&self) -> TileDBResult<Vec<Vec<Range>>> {
        let c_subarray = self.capi();
        let ndims = self.schema.domain()?.ndim()? as u32;
        let mut ranges: Vec<Vec<Range>> = Vec::new();
        for dim_idx in 0..ndims {
            let mut nranges: u64 = 0;
            self.capi_call(|ctx| unsafe {
                ffi::tiledb_subarray_get_range_num(
                    ctx,
                    c_subarray,
                    dim_idx,
                    &mut nranges,
                )
            })?;

            let dim = self.schema.domain()?.dimension(dim_idx)?;
            let var_sized_dim = dim.is_var_sized()?;

            let mut dim_ranges: Vec<Range> = Vec::new();
            for rng_idx in 0..nranges {
                if var_sized_dim {
                    let mut start_size: u64 = 0;
                    let mut end_size: u64 = 0;
                    self.capi_call(|ctx| unsafe {
                        ffi::tiledb_subarray_get_range_var_size(
                            ctx,
                            c_subarray,
                            dim_idx,
                            rng_idx,
                            &mut start_size,
                            &mut end_size,
                        )
                    })?;

                    /*
                     * SC-48075: SIGABRT when calling this function if there is no range set
                     * The SIGABRT should be fixed, but it's also fair that if no range is
                     * set we don't really have a way to produce an upper bound which
                     * must be arbitrarily long. Hence here we skip pushing a range.
                     */
                    if start_size > 0 || end_size > 0 {
                        let start =
                            vec![0u8; start_size as usize].into_boxed_slice();
                        let end =
                            vec![0u8; end_size as usize].into_boxed_slice();

                        self.capi_call(|ctx| unsafe {
                            ffi::tiledb_subarray_get_range_var(
                                ctx,
                                c_subarray,
                                dim_idx,
                                rng_idx,
                                start.as_ptr() as *mut std::ffi::c_void,
                                end.as_ptr() as *mut std::ffi::c_void,
                            )
                        })?;

                        let dtype = dim.datatype()?;
                        let cvn = dim.cell_val_num()?;
                        let range =
                            TypedRange::from_slices(dtype, cvn, &start, &end)?
                                .range;
                        dim_ranges.push(range);
                    } else {
                        /*
                         * See SC-48075 above.
                         * It's tempting to assert that `nranges == 1` but writing
                         * down a range that's empty on both ends is valid
                         */
                    }
                } else {
                    let dtype = dim.datatype()?;

                    // Apparently stride exists in the API but isn't used.
                    let mut stride: *const std::ffi::c_void = out_ptr!();

                    physical_type_go!(dtype, DT, {
                        let mut start_ptr: *const DT = out_ptr!();
                        let mut end_ptr: *const DT = out_ptr!();
                        self.capi_call(|ctx| unsafe {
                            ffi::tiledb_subarray_get_range(
                                ctx,
                                c_subarray,
                                dim_idx,
                                rng_idx,
                                &mut start_ptr as *mut *const DT
                                    as *mut *const std::ffi::c_void,
                                &mut end_ptr as *mut *const DT
                                    as *mut *const std::ffi::c_void,
                                &mut stride,
                            )
                        })?;

                        let (start, end) = unsafe { (*start_ptr, *end_ptr) };
                        let range = Range::from(&[start, end]);
                        dim_ranges.push(range);
                    })
                }
            }

            ranges.push(dim_ranges);
        }

        Ok(ranges)
    }
}

pub struct Builder<Q>
where
    Q: QueryBuilder + Sized,
{
    query: Q,
    raw: RawSubarray,
}

impl<Q> ContextBound for Builder<Q>
where
    Q: QueryBuilder,
{
    fn context(&self) -> Context {
        self.query.base().context()
    }
}

impl<Q> Builder<Q>
where
    Q: QueryBuilder + Sized,
{
    pub(crate) fn for_query(query: Q) -> TileDBResult<Self> {
        let context = query.base().context();
        let c_array = **query.base().carray();
        let mut c_subarray: *mut ffi::tiledb_subarray_t = out_ptr!();

        context.capi_call(|ctx| unsafe {
            ffi::tiledb_subarray_alloc(ctx, c_array, &mut c_subarray)
        })?;

        Ok(Builder {
            query,
            raw: RawSubarray::Owned(c_subarray),
        })
    }

    /// Add a range on a dimension to the subarray. Adding a range restricts
    /// how much data TileDB has to read from disk to complete a query.
    pub fn add_range<Key: Into<LookupKey> + Clone, IntoRange: Into<Range>>(
        self,
        key: Key,
        range: IntoRange,
    ) -> TileDBResult<Self> {
        // Get the dimension so that we can assert the correct Range type.
        let schema = self.query.base().query.array.schema()?;
        let dim = schema.domain()?.dimension(key.clone())?;

        let range = range.into();
        range
            .check_dimension_compatibility(dim.datatype()?, dim.cell_val_num()?)
            .map_err(|e| {
                Error::InvalidArgument(
                    anyhow!("Invalid range variant for dimension").context(e),
                )
            })?;

        let c_subarray = *self.raw;

        match range {
            Range::Single(range) => {
                single_value_range_go!(range, _DT, start, end, {
                    let start = start.to_le_bytes();
                    let end = end.to_le_bytes();
                    match key.into() {
                        LookupKey::Index(idx) => {
                            self.query.base().capi_call(|ctx| unsafe {
                                ffi::tiledb_subarray_add_range(
                                    ctx,
                                    c_subarray,
                                    idx as u32,
                                    start.as_ptr() as *const std::ffi::c_void,
                                    end.as_ptr() as *const std::ffi::c_void,
                                    std::ptr::null(),
                                )
                            })?;
                        }
                        LookupKey::Name(name) => {
                            let c_name = cstring!(name);
                            self.query.base().capi_call(|ctx| unsafe {
                                ffi::tiledb_subarray_add_range_by_name(
                                    ctx,
                                    c_subarray,
                                    c_name.as_ptr(),
                                    start.as_ptr() as *const std::ffi::c_void,
                                    end.as_ptr() as *const std::ffi::c_void,
                                    std::ptr::null(),
                                )
                            })?;
                        }
                    }
                })
            }
            Range::Multi(_) => unreachable!(
                "This is rejected by range.check_dimension_compatibility"
            ),
            Range::Var(range) => {
                var_value_range_go!(range, _DT, start, end, {
                    match key.into() {
                        LookupKey::Index(idx) => {
                            self.query.base().capi_call(|ctx| unsafe {
                                ffi::tiledb_subarray_add_range_var(
                                    ctx,
                                    c_subarray,
                                    idx as u32,
                                    start.as_ptr() as *const std::ffi::c_void,
                                    start.len() as u64,
                                    end.as_ptr() as *const std::ffi::c_void,
                                    end.len() as u64,
                                )
                            })?;
                        }
                        LookupKey::Name(name) => {
                            let c_name = cstring!(name);
                            self.query.base().capi_call(|ctx| unsafe {
                                ffi::tiledb_subarray_add_range_var_by_name(
                                    ctx,
                                    c_subarray,
                                    c_name.as_ptr(),
                                    start.as_ptr() as *const std::ffi::c_void,
                                    start.len() as u64,
                                    end.as_ptr() as *const std::ffi::c_void,
                                    end.len() as u64,
                                )
                            })?;
                        }
                    }
                })
            }
        }

        Ok(self)
    }

    /// Add a list of point ranges to the query.
    pub fn add_point_ranges<Key: Into<LookupKey>, T: PhysicalType>(
        self,
        key: Key,
        points: &[T],
    ) -> TileDBResult<Self> {
        let schema = self.query.base().query.array.schema()?;
        let dim_idx = schema.domain()?.dimension_index(key)?;
        let dim = schema.domain()?.dimension(dim_idx)?;
        let dtype = dim.datatype()?;
        if dtype.is_compatible_type::<T>() {
            return Err(Error::Datatype(DatatypeErrorKind::TypeMismatch {
                user_type: std::any::type_name::<T>().to_owned(),
                tiledb_type: dtype,
            }));
        }

        if points.is_empty() {
            return Err(Error::InvalidArgument(anyhow!(
                "No point ranges provided to set."
            )));
        }

        let ctx = self.query.base().context();
        let c_subarray = *self.raw;

        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_subarray_add_point_ranges(
                ctx,
                c_subarray,
                dim_idx as u32,
                points.as_ptr() as *const std::ffi::c_void,
                points.len() as u64,
            )
        })?;

        Ok(self)
    }

    /// Adds a set of ranges on each dimension.
    /// The outer `Vec` of `ranges` is the list of dimensions and the inner
    /// `Vec` is the list of requested ranges for that dimension.
    /// If a list is empty for a dimension, then all the coordinates of that
    /// dimension are selected.
    pub fn dimension_ranges(
        self,
        ranges: Vec<Vec<Range>>,
    ) -> TileDBResult<Self> {
        let mut b = self;
        for (d, ranges) in ranges.into_iter().enumerate() {
            for r in ranges.into_iter() {
                b = b.add_range(d, r)?;
            }
        }
        Ok(b)
    }

    /// Apply the subarray to the query, returning the query builder.
    pub fn finish_subarray(self) -> TileDBResult<Q> {
        let c_query = **self.query.base().cquery();
        let c_subarray = *self.raw;

        self.query.base().capi_call(|ctx| unsafe {
            ffi::tiledb_query_set_subarray_t(ctx, c_query, c_subarray)
        })?;
        Ok(self.query)
    }
}

/// Encapsulates data for a subarray.
#[derive(Clone, Debug, PartialEq)]
pub struct SubarrayData {
    /// List of requested ranges on each dimension.
    /// The outer `Vec` is the list of dimensions and the inner `Vec`
    /// is the list of requested ranges for that dimension.
    /// If a list is empty for a dimension, then all the coordinates
    /// of that dimension are selected.
    pub dimension_ranges: Vec<Vec<Range>>,
}

impl SubarrayData {
    /// Returns a new `SubarrayData` which represents the intersection
    /// of all the ranges of `self` with a new set of `ranges` on each dimension.
    ///
    /// If any dimension does not have any intersection with `ranges`, then
    /// this returns `None` as the resulting subarray would select no coordinates.
    pub fn intersect_ranges(&self, ranges: &[Range]) -> Option<Self> {
        let updated_ranges = self
            .dimension_ranges
            .iter()
            .zip(ranges.iter())
            .map(|(current_ranges, new_range)| {
                if current_ranges.is_empty() {
                    // empty means select the whole thing
                    vec![new_range.clone()]
                } else {
                    current_ranges
                        .iter()
                        .filter_map(|current_range| {
                            current_range.intersection(new_range)
                        })
                        .collect::<Vec<Range>>()
                }
            })
            .collect::<Vec<Vec<Range>>>();

        if updated_ranges.iter().any(|dim| dim.is_empty()) {
            None
        } else {
            Some(SubarrayData {
                dimension_ranges: updated_ranges,
            })
        }
    }

    /// Returns a new `SubarrayData` which represents the intersection
    /// of all the ranges of `self` with all of the ranges of `other` on each dimension.
    ///
    /// ```
    /// use tiledb::query::subarray::SubarrayData;
    /// use tiledb::range::Range;
    ///
    /// let s1 = SubarrayData {
    ///     dimension_ranges: vec![
    ///         vec![Range::from(&[0, 100]), Range::from(&[200, 300])],
    ///         vec![Range::from(&[2, 6]), Range::from(&[8, 12])],
    ///         vec![Range::from(&[20, 30]), Range::from(&[40, 50])]
    ///     ]
    /// };
    /// let s2 = SubarrayData {
    ///     dimension_ranges: vec![
    ///         vec![Range::from(&[150, 250])],
    ///         vec![Range::from(&[4, 10]), Range::from(&[12, 12])],
    ///         vec![Range::from(&[25, 45])]
    ///     ]
    /// };
    /// let intersection = s1.intersect(&s2);
    ///
    /// assert_eq!(intersection, Some(SubarrayData {
    ///     dimension_ranges: vec![
    ///         vec![Range::from(&[200, 250])],
    ///         vec![Range::from(&[4, 6]), Range::from(&[8, 10]), Range::from(&[12, 12])],
    ///         vec![Range::from(&[25, 30]), Range::from(&[40, 45])]
    ///     ]
    /// }));
    /// ```
    ///
    /// If any dimension does not have any intersection, then this returns `None`
    /// as the resulting subarray would select no coordinates.
    /// ```
    /// use tiledb::query::subarray::SubarrayData;
    /// use tiledb::range::Range;
    ///
    /// let s1 = SubarrayData {
    ///     dimension_ranges: vec![
    ///         vec![Range::from(&[50, 100]), Range::from(&[400, 450])]
    ///     ]
    /// };
    /// let s2 = SubarrayData {
    ///     dimension_ranges: vec![
    ///         vec![Range::from(&[150, 250]), Range::from(&[300, 350])],
    ///     ]
    /// };
    /// let intersection = s1.intersect(&s2);
    /// assert_eq!(intersection, None);
    /// ```
    pub fn intersect(&self, other: &SubarrayData) -> Option<Self> {
        let updated_ranges = self
            .dimension_ranges
            .iter()
            .zip(other.dimension_ranges.iter())
            .map(|(my_dimension, their_dimension)| {
                my_dimension
                    .iter()
                    .cartesian_product(their_dimension.iter())
                    .filter_map(|(rm, rt)| rm.intersection(rt))
                    .collect::<Vec<Range>>()
            })
            .collect::<Vec<Vec<Range>>>();

        if self
            .dimension_ranges
            .iter()
            .zip(updated_ranges.iter())
            .any(|(before, after)| after.is_empty() && !before.is_empty())
        {
            None
        } else {
            Some(SubarrayData {
                dimension_ranges: updated_ranges,
            })
        }
    }
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy {
    use std::rc::Rc;

    use proptest::prelude::*;

    use super::*;
    use crate::array::SchemaData;

    impl Arbitrary for SubarrayData {
        type Parameters = Option<Rc<SchemaData>>;
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
            let strat_dimension_ranges = if let Some(schema) = params {
                schema
                    .domain
                    .dimension
                    .iter()
                    .map(|d| d.subarray_strategy(None).unwrap())
                    .collect::<Vec<BoxedStrategy<Range>>>()
            } else {
                todo!()
            };

            const DIMENSION_MIN_RANGES: usize = 0;
            const DIMENSION_MAX_RANGES: usize = 4;

            strat_dimension_ranges
                .into_iter()
                .map(|strat_range| {
                    proptest::collection::vec(
                        strat_range,
                        DIMENSION_MIN_RANGES..=DIMENSION_MAX_RANGES,
                    )
                    .boxed()
                })
                .collect::<Vec<BoxedStrategy<Vec<Range>>>>()
                .prop_map(|dimension_ranges| SubarrayData { dimension_ranges })
                .boxed()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{DefaultHasher, Hash, Hasher};
    use std::rc::Rc;

    use itertools::izip;
    use proptest::prelude::*;
    use tiledb_test_utils::{self, TestArrayUri};

    use super::*;
    use crate::array::*;
    use crate::query::{
        Query, QueryBuilder, ReadBuilder, ReadQuery, ReadQueryBuilder,
        WriteBuilder,
    };
    use crate::Datatype;

    /// The default subarray of a query with a constrained dimension
    /// is whatever the dimension constraints are
    #[test]
    fn default_subarray_constrained() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();

        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;
        let test_uri =
            crate::array::tests::create_quickstart_dense(&test_uri, &ctx)?;

        let a = Array::open(&ctx, test_uri, Mode::Read)?;
        let b = ReadBuilder::new(a)?;

        // inspect builder in-progress subarray
        {
            let subarray = b.subarray()?;
            let ranges = subarray.ranges()?;
            assert_eq!(
                vec![
                    vec![Range::Single(SingleValueRange::Int32(1, 4))],
                    vec![Range::Single(SingleValueRange::Int32(1, 4))]
                ],
                ranges
            );
        }

        let q = b.build();

        // inspect query subarray
        {
            let subarray = q.subarray()?;
            let ranges = subarray.ranges()?;
            assert_eq!(
                vec![
                    vec![Range::Single(SingleValueRange::Int32(1, 4))],
                    vec![Range::Single(SingleValueRange::Int32(1, 4))]
                ],
                ranges
            );
        }

        Ok(())
    }

    /// The default subarray of a query with unconstrained dimension
    /// is anything goes. The array used here has one unconstrained
    /// string dimension and one constrained int dimension, so we
    /// should expect empty ranges for one dimension and the
    /// dimension domain for the other.
    #[test]
    fn default_subarray_unconstrained() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();

        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;
        let test_uri = crate::array::tests::create_quickstart_sparse_string(
            &test_uri, &ctx,
        )?;

        let a = Array::open(&ctx, test_uri, Mode::Read)?;
        let b = ReadBuilder::new(a)?;

        // inspect builder in-progress subarray
        {
            let subarray = b.subarray()?;
            let ranges = subarray.ranges()?;
            assert_eq!(2, ranges.len());
            assert!(
                ranges[0].is_empty(),
                "Expected empty ranges but found {:?}",
                ranges[0]
            );
            assert_eq!(
                ranges[1],
                vec![Range::Single(SingleValueRange::Int32(1, 4))]
            );
        }

        let q = b.build();

        // inspect query subarray
        {
            let subarray = q.subarray()?;
            let ranges = subarray.ranges()?;
            assert_eq!(2, ranges.len());
            assert!(
                ranges[0].is_empty(),
                "Expected empty ranges but found {:?}",
                ranges[0]
            );
            assert_eq!(
                ranges[1],
                vec![Range::Single(SingleValueRange::Int32(1, 4))]
            );
        }

        Ok(())
    }

    #[test]
    fn test_dense_ranges() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;
        test_ranges(&ctx, ArrayType::Dense, &test_uri)
    }

    #[test]
    fn test_sparse_ranges() -> TileDBResult<()> {
        let ctx = Context::new().unwrap();
        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))?;
        test_ranges(&ctx, ArrayType::Sparse, &test_uri)
    }

    fn test_ranges(
        ctx: &Context,
        atype: ArrayType,
        test_uri: &dyn TestArrayUri,
    ) -> TileDBResult<()> {
        let array_uri = create_array(ctx, atype, test_uri)?;
        let array = Array::open(ctx, array_uri, Mode::Read)?;
        let query = ReadBuilder::new(array)?
            .start_subarray()?
            .add_range("id", &[1, 2])?
            .add_range("id", &[4, 6])?
            .add_range("id", &[8, 10])?
            .finish_subarray()?
            .build();

        let subarray = query.subarray()?;
        let ranges = subarray.ranges()?;

        // There's only one dimension with ranges
        assert_eq!(ranges.len(), 1);

        // The single id dimension has three ranges.
        assert_eq!(ranges[0].len(), 3);

        let expect = vec![
            Range::from(&[1i32, 2]),
            Range::from(&[4i32, 6]),
            Range::from(&[8i32, 10]),
        ];
        assert_eq!(ranges[0], expect);

        Ok(())
    }

    /// Create a simple dense test array with a couple fragments to inspect.
    fn create_array(
        ctx: &Context,
        atype: ArrayType,
        test_uri: &dyn TestArrayUri,
    ) -> TileDBResult<String> {
        let array_uri = if atype == ArrayType::Dense {
            test_uri.with_path("range_test_dense")
        } else {
            test_uri.with_path("range_test_sparse")
        }
        .map_err(|e| Error::Other(e.to_string()))?;

        let domain = {
            let rows = DimensionBuilder::new(
                ctx,
                "id",
                Datatype::Int32,
                ([1, 10], 4),
            )?
            .build();

            DomainBuilder::new(ctx)?.add_dimension(rows)?.build()
        };

        let attr = AttributeBuilder::new(ctx, "attr", Datatype::Int32)?.build();
        let schema = SchemaBuilder::new(ctx, atype, domain)?
            .add_attribute(attr)?
            .build()?;

        Array::create(ctx, &array_uri, schema)?;

        Ok(array_uri)
    }

    fn do_subarray_intersect_ranges(subarray: &SubarrayData, ranges: &[Range]) {
        if let Some(intersection) = subarray.intersect_ranges(ranges) {
            assert_eq!(
                subarray.dimension_ranges.len(),
                intersection.dimension_ranges.len()
            );
            assert_eq!(subarray.dimension_ranges.len(), ranges.len());

            for (before, after, update) in izip!(
                subarray.dimension_ranges.iter(),
                intersection.dimension_ranges.iter(),
                ranges.iter()
            ) {
                if before.is_empty() {
                    assert_eq!(vec![update.clone()], *after);
                    continue;
                }

                assert!(after.len() <= before.len());

                let mut r_after = after.iter();
                for r_before in before.iter() {
                    if let Some(r) = r_before.intersection(update) {
                        assert_eq!(*r_after.next().unwrap(), r);
                    }
                }
                assert_eq!(None, r_after.next());
            }
        } else {
            // for at least one dimension, none of the ranges could have intersected
            let found_empty_intersection = subarray
                .dimension_ranges
                .iter()
                .zip(ranges.iter())
                .any(|(current, new)| {
                    if current.is_empty() {
                        false
                    } else {
                        current.iter().all(|r| r.intersection(new).is_none())
                    }
                });
            assert!(
                found_empty_intersection,
                "dimensions: {:?}",
                subarray
                    .dimension_ranges
                    .iter()
                    .zip(ranges.iter())
                    .map(|(d, r)| format!(
                        "({:?} && {:?} = {:?}",
                        d,
                        r,
                        d.iter()
                            .map(|dr| dr.intersection(r))
                            .collect::<Vec<Option<Range>>>()
                    ))
                    .collect::<Vec<_>>()
            );
        }
    }

    /// Validate the intersection of two subarrays.
    /// `s1` and `s2` are two subarrays for the same schema.
    fn do_subarray_intersect_subarray(s1: &SubarrayData, s2: &SubarrayData) {
        if let Some(intersection) = s1.intersect(s2) {
            for (di, ds1, ds2) in izip!(
                intersection.dimension_ranges.iter(),
                s1.dimension_ranges.iter(),
                s2.dimension_ranges.iter(),
            ) {
                // there must be some pair from (rs1, rs2) where di is the intersection
                for ri in di.iter() {
                    let found_input = ds1
                        .iter()
                        .cartesian_product(ds2.iter())
                        .any(|(rs1, rs2)| {
                            Some(ri) == rs1.intersection(rs2).as_ref()
                        });
                    assert!(found_input, "ri = {:?}", ri);
                }

                // and for all pairs (rs1, rs2), there must be some ri which covers
                for (rs1, rs2) in ds1.iter().cartesian_product(ds2.iter()) {
                    let Some(intersection) = rs1.intersection(rs2) else {
                        continue;
                    };

                    let found_output = di.iter().any(|ri| intersection == *ri);
                    assert!(
                        found_output,
                        "rs1 = {:?}, rs2 = {:?}, intersection = {:?}",
                        rs1, rs2, intersection
                    );
                }
            }
        } else {
            // for each least one dimension, none of the ranges of `s1`
            // intersected with any range from `s2`
            let found_empty_intersection = s1
                .dimension_ranges
                .iter()
                .zip(s2.dimension_ranges.iter())
                .any(|(ds1, ds2)| {
                    ds1.iter()
                        .cartesian_product(ds2.iter())
                        .all(|(rs1, rs2)| rs1.intersection(rs2).is_none())
                });
            assert!(found_empty_intersection);
        }
    }

    fn strat_subarray_intersect_ranges(
    ) -> impl Strategy<Value = (SubarrayData, Vec<Range>)> {
        use crate::array::domain::strategy::Requirements as DomainRequirements;
        use crate::array::schema::strategy::Requirements as SchemaRequirements;

        let req = Rc::new(SchemaRequirements {
            domain: Some(Rc::new(DomainRequirements {
                num_dimensions: 1..=1,
                ..Default::default()
            })),
            ..Default::default()
        });

        any_with::<SchemaData>(req).prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            (
                any_with::<SubarrayData>(Some(Rc::clone(&schema))),
                schema.domain.subarray_strategy(),
            )
        })
    }

    fn strat_subarray_intersect_subarray(
    ) -> impl Strategy<Value = (SubarrayData, SubarrayData)> {
        use crate::array::domain::strategy::Requirements as DomainRequirements;
        use crate::array::schema::strategy::Requirements as SchemaRequirements;

        let req = Rc::new(SchemaRequirements {
            domain: Some(Rc::new(DomainRequirements {
                num_dimensions: 1..=1,
                ..Default::default()
            })),
            ..Default::default()
        });

        any_with::<SchemaData>(req).prop_flat_map(|schema| {
            let schema = Rc::new(schema);
            let strat_subarray =
                any_with::<SubarrayData>(Some(Rc::clone(&schema)));
            (strat_subarray.clone(), strat_subarray.clone())
        })
    }

    proptest! {
        #[test]
        fn subarray_intersect_ranges((subarray, range) in strat_subarray_intersect_ranges()) {
            do_subarray_intersect_ranges(&subarray, &range)
        }

        #[test]
        fn subarray_intersect_subarray((s1, s2) in strat_subarray_intersect_subarray()) {
            do_subarray_intersect_subarray(&s1, &s2)
        }
    }

    #[test]
    fn dimension_ranges() {
        let ctx = Context::new().unwrap();

        let test_uri = tiledb_test_utils::get_uri_generator()
            .map_err(|e| Error::Other(e.to_string()))
            .unwrap();
        let test_uri = crate::array::tests::create_quickstart_sparse_string(
            &test_uri, &ctx,
        )
        .unwrap();

        let derive_att = |row: &str, col: &i32| -> i32 {
            let mut h = DefaultHasher::new();
            (row, col).hash(&mut h);
            h.finish() as i32
        };

        let row_values = ["foo", "bar", "baz", "quux", "gub"];
        let col_values = (1..=4).collect::<Vec<i32>>();

        // write some data
        {
            let (rows, (cols, atts)) = row_values
                .iter()
                .flat_map(|r| {
                    col_values.iter().map(move |c| {
                        let att = derive_att(r, c);
                        (r.to_string(), (*c, att))
                    })
                })
                .collect::<(Vec<String>, (Vec<i32>, Vec<i32>))>();

            let w = Array::open(&ctx, &test_uri, Mode::Write).unwrap();
            let q = WriteBuilder::new(w)
                .unwrap()
                .data("rows", &rows)
                .unwrap()
                .data("cols", &cols)
                .unwrap()
                .data("a", &atts)
                .unwrap()
                .build();

            q.submit().unwrap();
            q.finalize().unwrap();
        }

        let schema = {
            let array = Array::open(&ctx, &test_uri, Mode::Read).unwrap();
            Rc::new(SchemaData::try_from(array.schema().unwrap()).unwrap())
        };

        let do_dimension_ranges = |subarray: SubarrayData| -> TileDBResult<()> {
            let array = Array::open(&ctx, &test_uri, Mode::Read).unwrap();
            let mut q = ReadBuilder::new(array)?
                .start_subarray()?
                .dimension_ranges(subarray.dimension_ranges.clone())?
                .finish_subarray()?
                .register_constructor::<_, Vec<String>>(
                    "rows",
                    Default::default(),
                )?
                .register_constructor::<_, Vec<i32>>(
                    "cols",
                    Default::default(),
                )?
                .register_constructor::<_, Vec<i32>>("a", Default::default())?
                .build();

            let (atts, (cols, (rows, _))) = q.execute()?;
            assert_eq!(rows.len(), cols.len());
            assert_eq!(rows.len(), atts.len());

            // validate the number of results.
            // this is hard to do with multi ranges which might be overlapping
            // so skip for those cases. tiledb returns the union of subarray
            // ranges by default, so to be accurate we would have to do the union
            if subarray.dimension_ranges[0].len() <= 1
                && subarray.dimension_ranges[1].len() <= 1
            {
                let num_cells_0 = if subarray.dimension_ranges[0].is_empty() {
                    row_values.len()
                } else {
                    let Range::Var(VarValueRange::UInt8(ref lb, ref ub)) =
                        subarray.dimension_ranges[0][0]
                    else {
                        unreachable!()
                    };
                    row_values
                        .iter()
                        .filter(|row| {
                            lb.as_ref() <= row.as_bytes()
                                && row.as_bytes() <= ub.as_ref()
                        })
                        .count()
                };
                let num_cells_1 = if subarray.dimension_ranges[1].is_empty() {
                    col_values.len()
                } else {
                    subarray.dimension_ranges[1][0].num_cells().unwrap()
                        as usize
                };

                let expect_num_cells = num_cells_0 * num_cells_1;
                assert_eq!(expect_num_cells, rows.len());
            }

            for (row, col, att) in izip!(rows, cols, atts) {
                assert_eq!(att, derive_att(&row, &col));

                let row_in_bounds = subarray.dimension_ranges[0].is_empty()
                    || subarray.dimension_ranges[0].iter().any(|r| {
                        let Range::Var(VarValueRange::UInt8(ref lb, ref ub)) =
                            r
                        else {
                            unreachable!()
                        };
                        lb.as_ref() <= row.as_bytes()
                            && row.as_bytes() <= ub.as_ref()
                    });
                assert!(row_in_bounds);

                let col_in_bounds = subarray.dimension_ranges[1].is_empty()
                    || subarray.dimension_ranges[1].iter().any(|r| {
                        let Range::Single(SingleValueRange::Int32(lb, ub)) = r
                        else {
                            unreachable!()
                        };
                        *lb <= col && col <= *ub
                    });
                assert!(col_in_bounds);
            }

            Ok(())
        };

        proptest!(move |(subarray in any_with::<SubarrayData>(Some(Rc::clone(&schema))))| {
            do_dimension_ranges(subarray).expect("Read query error");
        })
    }
}
