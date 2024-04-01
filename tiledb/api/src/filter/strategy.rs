use std::collections::VecDeque;
use std::rc::Rc;

use proptest::prelude::*;
use proptest::strategy::Just;

use crate::array::{ArrayType, DomainData};
use crate::datatype::strategy::*;
use crate::filter::list::FilterListData;
use crate::filter::*;
use crate::Datatype;

#[derive(Clone)]
pub enum StrategyContext {
    Domain(ArrayType, Rc<DomainData>),
    SchemaCoordinates(Rc<DomainData>),
}

/// Defines requirements for what a generated filter must be able to accept
#[derive(Clone, Default)]
pub struct Requirements {
    pub input_datatype: Option<Datatype>,
    pub context: Option<StrategyContext>,
}

impl Requirements {
    /// @return true if and only if the webp filter type is allowed given the requirements.
    /// In an array schema, webp is allowed for attributes only if:
    /// - there are exactly two dimensions
    /// - the two dimensions have the same integral datatype
    /// - the array is a dense array
    fn ok_webp(&self) -> bool {
        let ok_datatype = match self.input_datatype {
            None => true,
            Some(Datatype::UInt8) => true,
            Some(_) => false,
        };
        if !ok_datatype {
            return false;
        }

        let ok_context = match self.context.as_ref() {
            Some(StrategyContext::Domain(array_type, domain)) => {
                if *array_type == ArrayType::Sparse {
                    false
                } else if domain.dimension.len() != 2 {
                    false
                } else if !domain.dimension[0].datatype.is_integral_type() {
                    false
                } else {
                    domain.dimension[0].datatype == domain.dimension[1].datatype
                }
            }
            _ => true,
        };

        ok_context
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

fn prop_compression_reinterpret_datatype() -> impl Strategy<Value = Datatype> {
    prop_datatype_implemented()
}

fn prop_compression(
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = FilterData> {
    const MIN_COMPRESSION_LEVEL: i32 = 1;
    const MAX_COMPRESSION_LEVEL: i32 = 9;

    prop_compression_reinterpret_datatype()
        .prop_flat_map(move |reinterpret_datatype| {
            let compression_types = vec![
                CompressionType::Bzip2,
                CompressionType::Dictionary,
                CompressionType::Gzip,
                CompressionType::Lz4,
                CompressionType::Rle,
                CompressionType::Zstd,
            ];

            let with_double_delta = || -> Vec<CompressionType> {
                let mut with_double_delta = compression_types.clone();
                with_double_delta.push(CompressionType::Delta);
                with_double_delta.push(CompressionType::DoubleDelta);
                with_double_delta
            };

            let mut ok_double_delta =
                match (requirements.input_datatype, reinterpret_datatype) {
                    (None, _) => true,
                    (Some(input_datatype), Datatype::Any) => {
                        !input_datatype.is_real_type()
                    }
                    (Some(_), reinterpret_datatype) => {
                        !reinterpret_datatype.is_real_type()
                    }
                };

            if ok_double_delta {
                if let Some(StrategyContext::SchemaCoordinates(ref domain)) =
                    requirements.context
                {
                    /*
                     * See tiledb/array_schema/array_schema.cc for the rules.
                     * DoubleDelta compressor is disallowed in the schema coordinates filter
                     * if there is a floating-point dimension.
                     */
                    ok_double_delta = !domain.dimension.iter().any(|d| {
                        d.datatype.is_real_type() && d.filters.is_empty()
                    })
                }
            }

            let kind = proptest::strategy::Union::new(
                if ok_double_delta {
                    with_double_delta()
                } else {
                    compression_types.clone()
                }
                .into_iter()
                .map(Just),
            );

            (
                kind,
                MIN_COMPRESSION_LEVEL..=MAX_COMPRESSION_LEVEL,
                Just(reinterpret_datatype),
            )
        })
        .prop_map(|(kind, level, reinterpret_datatype)| {
            FilterData::Compression(CompressionData {
                kind,
                level: Some(level),
                reinterpret_datatype: Some(reinterpret_datatype),
            })
        })
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

fn prop_webp() -> impl Strategy<Value = FilterData> {
    (
        prop_oneof![
            Just(WebPFilterInputFormat::None),
            Just(WebPFilterInputFormat::Rgb),
            Just(WebPFilterInputFormat::Bgr),
            Just(WebPFilterInputFormat::Rgba),
            Just(WebPFilterInputFormat::Bgra),
        ],
        prop_oneof![Just(false), Just(true)],
        0f32..=100f32,
    )
        .prop_map(|(input_format, lossless, quality)| FilterData::WebP {
            input_format: Some(input_format),
            lossless: Some(lossless),
            quality: Some(quality),
        })
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

    let ok_bit_reduction = match requirements.input_datatype {
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
        filter_strategies.push(prop_positivedelta().boxed());
    }

    filter_strategies.push(prop_compression(Rc::clone(&requirements)).boxed());

    let ok_scale_float = match requirements.input_datatype {
        None => true,
        Some(dt) => [std::mem::size_of::<f32>(), std::mem::size_of::<f64>()]
            .contains(&(dt.size() as usize)),
    };
    if ok_scale_float {
        filter_strategies.push(prop_scalefloat().boxed());
    }

    let ok_webp = requirements.ok_webp();
    if ok_webp {
        filter_strategies.push(prop_webp().boxed());
    }

    let ok_xor = match requirements.input_datatype {
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

fn prop_filter_pipeline_impl(
    requirements: Rc<Requirements>,
    nfilters: usize,
) -> impl Strategy<Value = VecDeque<FilterData>> {
    if nfilters == 0 {
        Just(VecDeque::new()).boxed()
    } else {
        prop_filter(Rc::clone(&requirements))
            .prop_flat_map(move |filter| {
                // This unwrap is guaranteed to succeed because the filter was
                // already checked before being returned from
                // prop_filter_for_datatype.
                let next = filter
                    .transform_datatype(&requirements.input_datatype.expect(
                        "Input datatype required to construct pipeline",
                    ))
                    .unwrap();
                let next_requirements = Requirements {
                    input_datatype: Some(next),
                    ..(*requirements).clone()
                };
                prop_filter_pipeline_impl(
                    Rc::new(next_requirements),
                    nfilters - 1,
                )
                .boxed()
                .prop_map(move |mut filter_vec| {
                    filter_vec.push_front(filter.clone());
                    filter_vec
                })
            })
            .boxed()
    }
}

pub fn prop_filter_pipeline(
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = FilterListData> {
    const MIN_FILTERS: usize = 0;
    const MAX_FILTERS: usize = 4;

    fn with_datatype(
        requirements: Rc<Requirements>,
    ) -> impl Strategy<Value = FilterListData> {
        (MIN_FILTERS..=MAX_FILTERS).prop_flat_map(move |nfilters| {
            prop_filter_pipeline_impl(Rc::clone(&requirements), nfilters)
                .prop_map(move |filter_deque| {
                    filter_deque.into_iter().collect::<FilterListData>()
                })
        })
    }

    if requirements.input_datatype.is_some() {
        with_datatype(Rc::clone(&requirements)).boxed()
    } else {
        prop_datatype_implemented()
            .prop_flat_map(move |input_datatype| {
                with_datatype(Rc::new(Requirements {
                    input_datatype: Some(input_datatype),
                    ..(*requirements).clone()
                }))
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Factory};

    #[test]
    /// Test that the arbitrary filter construction always succeeds
    fn filter_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(filt in prop_filter_pipeline(Default::default()))| {
            filt.create(&ctx).expect("Error constructing arbitrary filter");
        });
    }

    /// Test that the arbitrary filter construction always succeeds with a
    /// supplied datatype
    #[test]
    fn filter_arbitrary_for_datatype() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|((dt, filt) in prop_datatype().prop_flat_map(
                |dt| (Just(dt), prop_filter(Rc::new(Requirements {
                    input_datatype: Some(dt),
                    ..Default::default()
                })))))| {
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

        proptest!(|(fl in prop_filter_pipeline(Default::default()))| {
            fl.create(&ctx).expect("Error constructing arbitrary filter list");
        });
    }

    #[test]
    /// Test that the arbitrary filter list construction always succeeds with a
    /// supplied datatype
    fn filter_list_arbitrary_for_datatype() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|((dt, fl) in prop_datatype_implemented().prop_flat_map(
                |dt| (Just(dt), prop_filter_pipeline(Rc::new(Requirements {
                    input_datatype: Some(dt),
                    ..Default::default()
                })))))| {
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
                    panic!("Constructed invalid filter list: \
                        {:?}, invalid at position {}", fl, fi)
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

        proptest!(|(attr in prop_filter_pipeline(Default::default()))| {
            let attr = attr.create(&ctx)
                .expect("Error constructing arbitrary filter");
            assert_eq!(attr, attr);
        });
    }
}
