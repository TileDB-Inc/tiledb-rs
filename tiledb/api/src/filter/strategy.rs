use std::rc::Rc;

use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;

use crate::array::{ArrayType, CellValNum, DomainData};
use crate::datatype::strategy::DatatypeContext;
use crate::dimension_constraints_go;
use crate::filter::list::FilterListData;
use crate::filter::*;

#[derive(Clone, Debug)]
pub enum StrategyContext {
    Attribute(Datatype, CellValNum),
    Dimension(Datatype, CellValNum),
    SchemaAttribute(Datatype, CellValNum, ArrayType, Rc<DomainData>),
    SchemaCoordinates(Rc<DomainData>),
}

impl StrategyContext {
    /// Returns the input to the filter pipeline
    /// (whereas `input_datatype` is the input to the current filter)
    pub fn pipeline_input_datatypes(
        &self,
    ) -> Option<Vec<(Datatype, Option<CellValNum>)>> {
        match self {
            StrategyContext::Attribute(dt, cvn) => {
                Some(vec![(*dt, Some(*cvn))])
            }
            StrategyContext::Dimension(dt, cvn) => {
                Some(vec![(*dt, Some(*cvn))])
            }
            StrategyContext::SchemaAttribute(dt, cvn, _, _) => {
                Some(vec![(*dt, Some(*cvn))])
            }
            StrategyContext::SchemaCoordinates(domain) => Some(
                domain
                    .dimension
                    .iter()
                    .map(|d| (d.datatype, d.cell_val_num))
                    .collect::<Vec<(Datatype, Option<CellValNum>)>>(),
            ),
        }
    }
}

/// Defines requirements for what a generated filter must be able to accept
#[derive(Clone, Debug)]
pub struct Requirements {
    pub input_datatype: Option<Datatype>,
    pub context: Option<StrategyContext>,
    pub pipeline_position: Option<usize>,
    pub allow_bit_reduction: bool,
    pub allow_positive_delta: bool,
    pub allow_scale_float: bool,
    pub allow_xor: bool,
    pub allow_compression_rle: bool,
    pub allow_compression_dict: bool,
    pub allow_compression_delta: bool,
}

impl Requirements {
    // SC-47328: assertion failed in XOR filter
    // Likely due to an earlier compression filter, but in general the threading of dataytpe
    // through a filter pipeline looks suspicious, so until analysis from the
    // core team clarifies, we will only allow XOR in the first filter position.
    // There are other instances of the datatypes not being correct either.
    // Ideally the datatype transformations would be adequate to construct a correct
    // pipeline, but until they are some filters much check if they are at the beginning of the
    // pipeline.
    pub fn begins_pipeline(&self) -> bool {
        matches!(self.pipeline_position, None | Some(0))
    }

    pub fn is_rle_viable(&self) -> bool {
        if !self.allow_compression_rle {
            false
        } else if self.pipeline_position.unwrap_or(0) == 0 {
            true
        } else if let Some(dts) = self
            .context
            .as_ref()
            .and_then(|c| c.pipeline_input_datatypes())
        {
            !dts.into_iter().any(|(dt, cvn)| {
                cvn == Some(CellValNum::Var) && dt.is_string_type()
            })
        } else {
            true
        }
    }
}

impl Default for Requirements {
    fn default() -> Self {
        Requirements {
            input_datatype: None,
            context: None,
            pipeline_position: None,
            allow_bit_reduction: true,
            allow_positive_delta: true,
            allow_scale_float: true,
            allow_xor: true,
            allow_compression_rle: true,
            allow_compression_dict: true,
            allow_compression_delta: true,
        }
    }
}

fn prop_bitwidthreduction() -> impl Strategy<Value = FilterData> {
    const MIN_WINDOW: u32 = 8;
    const MAX_WINDOW: u32 = 1024;
    prop_oneof![
        Just(FilterData::BitWidthReduction { max_window: None }),
        (MIN_WINDOW..=MAX_WINDOW).prop_map(|max_window| {
            FilterData::BitWidthReduction {
                max_window: Some(max_window),
            }
        })
    ]
}

fn prop_compression_delta_strategies(
    input_datatype: Option<Datatype>,
) -> Vec<BoxedStrategy<CompressionType>> {
    fn validate_type(
        in_dtype: &Option<Datatype>,
        reinterpret_dtype: &Datatype,
    ) -> bool {
        if let Some(in_dtype) = in_dtype {
            // If we're reinterpreting away from a floating point input,
            // the reinterpreted type must not be Any.
            if in_dtype.is_real_type() && *reinterpret_dtype == Datatype::Any {
                return false;
            }

            // The reinterpreted type can never be real
            if reinterpret_dtype.is_real_type() {
                return false;
            }

            // The delta filter requires that after reinterpreting the
            // datatype that the resulting buffer size is a evenly divisible
            // by the new type. The easiest way for us to guarantee that
            // is to only allow for reinterpreting to types that have a
            // smaller width that evenly divide the larger input type size.
            if in_dtype.size() % reinterpret_dtype.size() != 0 {
                return false;
            }
        } else {
            // The reinterpret datatype can never be a real type.
            if reinterpret_dtype.is_real_type() {
                return false;
            }
        }

        true
    }

    /* any non-float type is allowed */
    let delta =
        any_with::<Datatype>(DatatypeContext::DeltaFilterReinterpretDatatype)
            .prop_filter("Invalid reinterpret_datatype", move |dt| {
                validate_type(&input_datatype, dt)
            })
            .prop_map(|dt| CompressionType::Delta {
                reinterpret_datatype: Some(dt),
            });
    let double_delta =
        any_with::<Datatype>(DatatypeContext::DeltaFilterReinterpretDatatype)
            .prop_filter("Invalid reinterpret_datatype", move |dt| {
                validate_type(&input_datatype, dt)
            })
            .prop_map(|dt| CompressionType::DoubleDelta {
                reinterpret_datatype: Some(dt),
            });

    vec![delta.boxed(), double_delta.boxed()]
}

fn prop_compression(
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = FilterData> {
    const MIN_COMPRESSION_LEVEL: i32 = 1;
    const MAX_COMPRESSION_LEVEL: i32 = 9;

    // always availalble
    let compression_types = vec![
        CompressionType::Bzip2,
        CompressionType::Gzip,
        CompressionType::Lz4,
        CompressionType::Zstd,
    ];

    let try_rle =
        requirements.allow_compression_rle && requirements.is_rle_viable();
    let try_dict =
        requirements.allow_compression_dict && requirements.is_rle_viable();

    let try_delta = requirements.allow_compression_delta
        && if requirements.begins_pipeline() {
            if let Some(StrategyContext::SchemaCoordinates(ref domain)) =
                requirements.context
            {
                /*
                 * See tiledb/array_schema/array_schema.cc for the rules.
                 * DoubleDelta compressor is disallowed in the schema coordinates filter
                 * if there is a floating-point dimension
                 */
                !domain.dimension.iter().any(|d| {
                    d.datatype.is_real_type()
                        && d.filters
                            .as_ref()
                            .map(|fl| fl.is_empty())
                            .unwrap_or(true)
                })
            } else {
                true
            }
        } else {
            false
        };

    let strat_kind = {
        let mut strats = compression_types
            .into_iter()
            .map(|ct| Just(ct).boxed())
            .collect::<Vec<BoxedStrategy<CompressionType>>>();
        if try_rle {
            strats.push(Just(CompressionType::Rle).boxed());
        }
        if try_dict {
            strats.push(Just(CompressionType::Dictionary).boxed());
        }
        if try_delta {
            for strat in
                prop_compression_delta_strategies(requirements.input_datatype)
            {
                strats.push(strat);
            }
        }

        proptest::strategy::Union::new(strats).boxed()
    };

    (strat_kind, MIN_COMPRESSION_LEVEL..=MAX_COMPRESSION_LEVEL).prop_map(
        |(kind, level)| {
            FilterData::Compression(CompressionData {
                kind,
                level: Some(level),
            })
        },
    )
}

fn prop_positivedelta() -> impl Strategy<Value = FilterData> {
    const MIN_WINDOW: u32 = 8;
    const MAX_WINDOW: u32 = 1024;

    (MIN_WINDOW..=MAX_WINDOW).prop_map(|max_window| FilterData::PositiveDelta {
        max_window: Some(max_window),
    })
}

fn prop_scalefloat() -> impl Strategy<Value = FilterData> {
    (
        prop_oneof![
            Just(ScaleFloatByteWidth::I8),
            Just(ScaleFloatByteWidth::I16),
            Just(ScaleFloatByteWidth::I32),
            Just(ScaleFloatByteWidth::I64),
        ],
        proptest::num::f64::NORMAL,
        proptest::num::f64::NORMAL,
    )
        .prop_map(|(byte_width, factor, offset)| FilterData::ScaleFloat {
            byte_width: Some(byte_width),
            factor: Some(factor),
            offset: Some(offset),
        })
}

/// If conditions allow, return a strategy which generates an arbitrary WebP filter.
/// In an array schema, webp is allowed for attributes only if:
/// - the attribute type is UInt8
/// - there are exactly two dimensions
/// - the two dimensions have the same integral datatype
/// - the array is a dense array
///
/// Note that this probably could be more permissive returning Some in other non-Domain scenarios.
fn prop_webp(
    requirements: &Rc<Requirements>,
) -> Option<impl Strategy<Value = FilterData>> {
    if let Some(StrategyContext::SchemaAttribute(
        attribute_type,
        _,
        array_type,
        ref domain,
    )) = requirements.context.as_ref()
    {
        if *attribute_type != Datatype::UInt8
            || requirements.input_datatype != Some(Datatype::UInt8)
            || *array_type == ArrayType::Sparse
            || domain.dimension.len() != 2
            || !domain.dimension[0].datatype.is_integral_type()
            || domain.dimension[0].datatype != domain.dimension[1].datatype
        {
            return None;
        }

        const MAX_EXTENT: usize = 16383;

        dimension_constraints_go!(
            domain.dimension[0].constraints,
            _DT,
            _range,
            extent,
            {
                extent.filter(|ext| *ext as usize <= MAX_EXTENT)?;
            },
            return None
        );

        let mut formats: Vec<WebPFilterInputFormat> = vec![];
        dimension_constraints_go!(
            domain.dimension[1].constraints,
            _DT,
            _range,
            extent,
            {
                if let Some(extent) = extent {
                    let extent = extent as usize;
                    if extent / 3 <= MAX_EXTENT && extent % 3 == 0 {
                        formats.push(WebPFilterInputFormat::Rgb);
                        formats.push(WebPFilterInputFormat::Bgr);
                    }
                    if extent / 4 <= MAX_EXTENT && extent % 4 == 0 {
                        formats.push(WebPFilterInputFormat::Rgba);
                        formats.push(WebPFilterInputFormat::Bgra);
                    }
                } else {
                    return None;
                }
            },
            return None
        );

        if formats.is_empty() {
            return None;
        }

        Some(
            (
                proptest::strategy::Union::new(formats.into_iter().map(Just)),
                prop_oneof![Just(false), Just(true)],
                0f32..=100f32,
            )
                .prop_map(|(input_format, lossless, quality)| {
                    FilterData::WebP {
                        input_format,
                        lossless: Some(lossless),
                        quality: Some(quality),
                    }
                }),
        )
    } else {
        None
    }
}

pub fn prop_filter(
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = FilterData> {
    let mut filter_strategies = vec![
        Just(FilterData::BitShuffle).boxed(),
        Just(FilterData::ByteShuffle).boxed(),
        Just(FilterData::Checksum(ChecksumType::Md5)).boxed(),
        Just(FilterData::Checksum(ChecksumType::Sha256)).boxed(),
    ];

    let ok_bit_reduction = requirements.allow_bit_reduction
        && match requirements.input_datatype {
            None => true,
            Some(dt) => {
                dt.is_integral_type()
                    || dt.is_datetime_type()
                    || dt.is_time_type()
                    || dt.is_byte_type()
            }
        };
    if ok_bit_reduction {
        filter_strategies.push(prop_bitwidthreduction().boxed());
    }

    let ok_positive_delta = requirements.allow_positive_delta
        && match requirements.input_datatype {
            None => true,
            Some(dt) => {
                dt.is_integral_type()
                    || dt.is_datetime_type()
                    || dt.is_time_type()
                    || dt.is_byte_type()
            }
        };
    if ok_positive_delta {
        filter_strategies.push(prop_positivedelta().boxed());
    }

    filter_strategies.push(prop_compression(Rc::clone(&requirements)).boxed());

    let ok_scale_float = requirements.allow_scale_float
        && requirements.begins_pipeline()
        && match requirements.input_datatype {
            None => true,
            Some(dt) => {
                [std::mem::size_of::<f32>(), std::mem::size_of::<f64>()]
                    .contains(&(dt.size() as usize))
            }
        };
    if ok_scale_float {
        filter_strategies.push(prop_scalefloat().boxed());
    }

    if let Some(webp) = prop_webp(&requirements) {
        filter_strategies.push(webp.boxed());
    }

    let ok_xor = requirements.allow_xor
        && match requirements.input_datatype {
            Some(input_datatype) => {
                [1, 2, 4, 8].contains(&(input_datatype.size() as usize))
            }
            None => true,
        };
    if ok_xor {
        filter_strategies.push(Just(FilterData::Xor).boxed());
    }

    proptest::strategy::Union::new(filter_strategies)
}

/// Value tree to search through the complexity space of some filter pipeline.
///
/// A filter pipeline has a bit more structure than just a list of filters,
/// because the output of each filter feeds into the next one.
/// The input type is fixed, but the final output can be any data type.
///
/// The complexity search space is more restricted than a generic vector strategy.
/// 1) the filters themselves are basically scalars, so we don't need to shrink them
/// 2) we must preserve the soundness of the pipeline with contiguous elements,
///    so our only option is to delete from (or restore) the back of the pipeline
#[derive(Debug)]
pub struct FilterPipelineValueTree {
    initial_pipeline: FilterListData,
    sublen: usize,
}

impl FilterPipelineValueTree {
    pub fn new(init: FilterListData) -> Self {
        let sublen = init.len();
        FilterPipelineValueTree {
            initial_pipeline: init,
            sublen,
        }
    }
}

impl ValueTree for FilterPipelineValueTree {
    type Value = FilterListData;

    fn current(&self) -> Self::Value {
        self.initial_pipeline
            .iter()
            .take(self.sublen)
            .cloned()
            .collect::<FilterListData>()
    }

    fn simplify(&mut self) -> bool {
        if self.sublen > 0 {
            self.sublen -= 1;
            true
        } else {
            false
        }
    }

    fn complicate(&mut self) -> bool {
        if self.sublen < self.initial_pipeline.len() {
            self.sublen += 1;
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct FilterPipelineStrategy {
    requirements: Rc<Requirements>,
}

impl Strategy for FilterPipelineStrategy {
    type Tree = FilterPipelineValueTree;
    type Value = FilterListData;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        const MIN_FILTERS: usize = 0;
        const MAX_FILTERS: usize = 4;

        let nfilters = (MIN_FILTERS..=MAX_FILTERS).new_tree(runner)?.current();

        let input_datatype = if let Some(dt) = self.requirements.input_datatype
        {
            dt
        } else {
            any::<Datatype>().new_tree(runner)?.current()
        };

        let initial_pipeline = {
            let mut filters = vec![];
            let mut input_datatype = Some(input_datatype);
            for i in 0..nfilters {
                let req = Requirements {
                    input_datatype,
                    context: self.requirements.context.clone(),
                    pipeline_position: Some(i),
                    ..self.requirements.as_ref().clone()
                };

                let f = prop_filter(Rc::new(req)).new_tree(runner)?.current();

                // If the transformed datatype is None then we have a bug.
                // Do not panic here, that will swallow what the pipeline looked like.
                // Let the unit test fail and print the input.
                let output_datatype =
                    input_datatype.and_then(|dt| f.transform_datatype(&dt));
                input_datatype = output_datatype;

                filters.push(f);
            }

            filters.into_iter().collect::<FilterListData>()
        };

        Ok(FilterPipelineValueTree::new(initial_pipeline))
    }
}

impl Arbitrary for FilterListData {
    type Parameters = Rc<Requirements>;
    type Strategy = FilterPipelineStrategy;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        FilterPipelineStrategy {
            requirements: Rc::clone(&args),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Factory;
    use util::assert_option_subset;

    #[test]
    /// Test that the arbitrary filter construction always succeeds
    fn filter_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(filt in any::<FilterListData>())| {
            filt.create(&ctx).expect("Error constructing arbitrary filter");
        });
    }

    /// Test that the arbitrary filter construction always succeeds with a
    /// supplied datatype
    #[test]
    fn filter_arbitrary_for_datatype() {
        let ctx = Context::new().expect("Error creating context");

        let strat = any::<Datatype>().prop_flat_map(|dt| {
            (
                Just(dt),
                prop_filter(Rc::new(Requirements {
                    input_datatype: Some(dt),
                    ..Default::default()
                })),
            )
        });

        proptest!(|((dt, filt) in strat)| {
            let filt = filt.create(&ctx)
                .expect("Error constructing arbitrary filter");

            let filt_data = filt.filter_data()
                .expect("Error reading filter data");
            assert!(filt_data.transform_datatype(&dt).is_some());
        });
    }

    #[test]
    /// Test that the arbitrary filter list construction always succeeds
    fn filter_list_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(fl in any::<FilterListData>())| {
            fl.create(&ctx).expect("Error constructing arbitrary filter list");
        });
    }

    #[test]
    /// Test that the arbitrary filter list construction always succeeds with a
    /// supplied datatype
    fn filter_list_arbitrary_for_datatype() {
        let ctx = Context::new().expect("Error creating context");

        let strat = any::<Datatype>().prop_flat_map(|dt| {
            let req = Rc::new(Requirements {
                input_datatype: Some(dt),
                ..Default::default()
            });
            (Just(dt), any_with::<FilterListData>(req))
        });

        proptest!(|((dt, fl) in strat)| {
            let fl = fl.create(&ctx)
                .expect("Error constructing arbitrary filter");

            let mut current_dt = dt;

            let fl = fl.to_vec().expect("Error collecting filters");
            for (fi, f) in fl.iter().enumerate() {
                if let Some(next_dt) = f.filter_data()
                    .expect("Error reading filter data")
                    .transform_datatype(&current_dt) {
                        current_dt = next_dt
                } else {
                    panic!("Constructed invalid filter list for datatype {}: \
                        {:?}, invalid at position {}", dt, fl, fi)
                }
            }
        });
    }

    /// Test that ScaleFloat serialization is invertible, because floating
    /// point sadness
    #[test]
    fn filter_scalefloat_serde() {
        proptest!(|(scalefloat_in in prop_scalefloat())| {
            let json = serde_json::to_string(&scalefloat_in)
                .expect("Error serializing");
            let scalefloat_out = serde_json::from_str(&json)
                .expect("Error deserializing");
            assert_eq!(scalefloat_in, scalefloat_out);
        });
    }

    #[test]
    fn filter_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(pipeline in any::<FilterListData>())| {
            assert_eq!(pipeline, pipeline);
            assert_option_subset!(pipeline, pipeline);

            let pipeline = pipeline.create(&ctx)
                .expect("Error constructing arbitrary filter");
            assert_eq!(pipeline, pipeline);
        });
    }

    /// Ensure that filter pipelines can shrink
    #[test]
    fn pipeline_shrinking() {
        let strat = any::<FilterListData>();

        let mut runner =
            proptest::test_runner::TestRunner::new(Default::default());

        let mut value = loop {
            let value = strat.new_tree(&mut runner).unwrap();
            if value.current().len() > 2 {
                break value;
            }
        };

        let init = value.current();
        while value.simplify() {
            assert!(value.current().len() < init.len());
        }
    }
}
