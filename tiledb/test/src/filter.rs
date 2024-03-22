use proptest::prelude::*;
use proptest::strategy::Just;
use tiledb::context::Context;
use tiledb::filter::*;
use tiledb::filter_list::FilterList;
use tiledb::{Datatype, Result as TileDBResult};

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

    (MIN_WINDOW..=MAX_WINDOW)
        .prop_map(|max_window| FilterData::PositiveDelta { max_window })
}

pub fn arbitrary_scalefloat() -> impl Strategy<Value = FilterData> {
    (
        prop_oneof![
            Just(std::mem::size_of::<i8>()),
            Just(std::mem::size_of::<i16>()),
            Just(std::mem::size_of::<i32>()),
            Just(std::mem::size_of::<i64>()),
        ],
        proptest::num::f64::NORMAL,
        proptest::num::f64::NORMAL,
    )
        .prop_map(|(byte_width, factor, offset)| FilterData::ScaleFloat {
            byte_width: byte_width as u64,
            factor,
            offset,
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
            input_format,
            lossless,
            quality,
        })
}

pub fn arbitrary(
    context: &Context,
) -> impl Strategy<Value = TileDBResult<Filter>> {
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
    .prop_map(|filter| Filter::create(context, filter))
}

pub fn arbitrary_list(
    context: &Context,
) -> impl Strategy<Value = TileDBResult<FilterList>> {
    const MIN_FILTERS: usize = 0;
    const MAX_FILTERS: usize = 4;

    proptest::collection::vec(arbitrary(context), MIN_FILTERS..=MAX_FILTERS)
        .prop_map(|filters| {
            let mut b = tiledb::filter_list::Builder::new(context)?;
            for filter in filters {
                b = b.add_filter(filter?)?;
            }
            Ok(b.build())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that the arbitrary attribute construction always succeeds
    #[test]
    fn filter_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(_ in arbitrary(&ctx))| {});
    }

    #[test]
    fn filter_list_arbitrary() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(_ in arbitrary_list(&ctx))| {});
    }

    #[test]
    fn filter_eq_reflexivity() {
        let ctx = Context::new().expect("Error creating context");

        proptest!(|(attr in arbitrary(&ctx))| {
            let attr = attr.expect("Error constructing arbitrary filter");
            assert_eq!(attr, attr);
        });
    }
}
