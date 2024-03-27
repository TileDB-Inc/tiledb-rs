use std::collections::VecDeque;

use proptest::prelude::*;
use proptest::strategy::Just;

use tiledb::filter::*;
use tiledb::filter_list::FilterListData;
use tiledb::Datatype;

use crate::strategy::LifetimeBoundStrategy;

pub fn arbitrary_bitwidthreduction() -> impl Strategy<Value = FilterData> {
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

pub fn arbitrary_compression_reinterpret_datatype(
) -> impl Strategy<Value = Datatype> {
    crate::datatype::arbitrary_implemented()
}

pub fn arbitrary_compression() -> impl Strategy<Value = FilterData> {
    const MIN_COMPRESSION_LEVEL: i32 = 1;
    const MAX_COMPRESSION_LEVEL: i32 = 9;
    (
        prop_oneof![
            Just(CompressionType::Bzip2),
            Just(CompressionType::Delta),
            Just(CompressionType::Dictionary),
            Just(CompressionType::DoubleDelta),
            Just(CompressionType::Gzip),
            Just(CompressionType::Lz4),
            Just(CompressionType::Rle),
            Just(CompressionType::Zstd),
        ],
        MIN_COMPRESSION_LEVEL..=MAX_COMPRESSION_LEVEL,
        arbitrary_compression_reinterpret_datatype(),
    )
        .prop_map(|(kind, level, reinterpret_datatype)| {
            FilterData::Compression(CompressionData {
                kind,
                level: Some(level),
                reinterpret_datatype: Some(reinterpret_datatype),
            })
        })
}

pub fn arbitrary_positivedelta() -> impl Strategy<Value = FilterData> {
    const MIN_WINDOW: u32 = 8;
    const MAX_WINDOW: u32 = 1024;

    (MIN_WINDOW..=MAX_WINDOW).prop_map(|max_window| FilterData::PositiveDelta {
        max_window: Some(max_window),
    })
}

pub fn arbitrary_scalefloat() -> impl Strategy<Value = FilterData> {
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

pub fn arbitrary_webp() -> impl Strategy<Value = FilterData> {
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

pub fn arbitrary() -> impl Strategy<Value = FilterData> {
    prop_oneof![
        Just(FilterData::BitShuffle),
        Just(FilterData::ByteShuffle),
        arbitrary_bitwidthreduction(),
        Just(FilterData::Checksum(ChecksumType::Md5)),
        Just(FilterData::Checksum(ChecksumType::Sha256)),
        arbitrary_compression(),
        arbitrary_positivedelta(),
        arbitrary_scalefloat(),
        arbitrary_webp(),
        Just(FilterData::Xor)
    ]
}

pub fn arbitrary_for_datatype(
    input_datatype: Datatype,
) -> impl Strategy<Value = FilterData> {
    arbitrary()
        .prop_filter("Filter does not accept input type", move |filter| {
            filter.transform_datatype(&input_datatype).is_some()
        })
}

fn arbitrary_pipeline(
    start: Datatype,
    nfilters: usize,
) -> impl Strategy<Value = VecDeque<FilterData>> {
    if nfilters == 0 {
        Just(VecDeque::new()).boxed()
    } else {
        arbitrary_for_datatype(start)
            .prop_flat_map(move |filter| {
                /* the transform type must be Some per filter in `arbitrary` */
                let next = filter.transform_datatype(&start).unwrap();
                arbitrary_pipeline(next, nfilters - 1).bind().prop_map(
                    move |mut filter_vec| {
                        filter_vec.push_front(filter.clone());
                        filter_vec
                    },
                )
            })
            .boxed()
    }
}

pub fn arbitrary_list_for_datatype(
    datatype: Datatype,
) -> impl Strategy<Value = FilterListData> {
    const MIN_FILTERS: usize = 0;
    const MAX_FILTERS: usize = 4;

    (MIN_FILTERS..=MAX_FILTERS).prop_flat_map(move |nfilters| {
        arbitrary_pipeline(datatype, nfilters).prop_map(move |filter_deque| {
            let mut current_dt = datatype;
            for filter in filter_deque.iter() {
                current_dt = if let Some(next_dt) =
                    filter.transform_datatype(&current_dt)
                {
                    next_dt
                } else {
                    unreachable!("Error in filter pipeline construction: {:?} does not accept input type {} in pipeline {:?}",
                        filter, current_dt, filter_deque)
                }
            }
            filter_deque.into_iter().collect::<FilterListData>()
        })
    })
}

pub fn arbitrary_list() -> impl Strategy<Value = FilterListData> {
    crate::datatype::arbitrary().prop_flat_map(arbitrary_list_for_datatype)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiledb::{Context, Factory};

    #[test]
    /// Test that the arbitrary filter construction always succeeds
    fn filter_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(filt in arbitrary())| {
            filt.create(&ctx).expect("Error constructing arbitrary filter");
        });
    }

    /// Test that the arbitrary filter construction always succeeds with a supplied datatype
    #[test]
    fn filter_arbitrary_for_datatype() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|((dt, filt) in crate::datatype::arbitrary().prop_flat_map(|dt| (Just(dt), arbitrary_for_datatype(dt))))| {
            let filt = filt.create(&ctx).expect("Error constructing arbitrary filter");

            let filt_data = filt.filter_data().expect("Error reading filter data");
            assert!(filt_data.transform_datatype(&dt).is_some());
        });
    }

    #[test]
    /// Test that the arbitrary filter list construction always succeeds
    fn filter_list_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(fl in arbitrary_list())| {
            fl.create(&ctx).expect("Error constructing arbitrary filter list");
        });
    }

    #[test]
    /// Test that the arbitrary filter list construction always succeeds with a supplied datatype
    fn filter_list_arbitrary_for_datatype() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|((dt, fl) in crate::datatype::arbitrary_implemented().prop_flat_map(|dt| (Just(dt), arbitrary_list_for_datatype(dt))))| {
            let fl = fl.create(&ctx).expect("Error constructing arbitrary filter");

            let mut current_dt = dt;

            let fl = fl.to_vec().expect("Error collecting filters");
            for (fi, f) in fl.iter().enumerate() {
                if let Some(next_dt) = f.filter_data().expect("Error reading filter data").transform_datatype(&current_dt) {
                    current_dt = next_dt
                } else {
                    panic!("Constructed invalid filter list: {:?}, invalid at position {}", fl, fi)
                }
            }
        });
    }

    /// Test that ScaleFloat serialization is invertible, because floating point sadness
    #[test]
    fn filter_scalefloat_serde() {
        proptest!(|(scalefloat_in in arbitrary_scalefloat())| {
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

        proptest!(|(attr in arbitrary())| {
            let attr = attr.create(&ctx).expect("Error constructing arbitrary filter");
            assert_eq!(attr, attr);
        });
    }
}
