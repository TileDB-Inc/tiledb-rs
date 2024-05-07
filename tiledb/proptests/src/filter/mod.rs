use proptest::prelude::*;

use tiledb::array::schema::CellValNum;
use tiledb::datatype::Datatype;
use tiledb::filter::{
    ChecksumType, CompressionData, CompressionType, FilterData,
    ScaleFloatByteWidth, WebPFilterInputFormat,
};

use crate::datatype as pt_datatype;

pub mod list;

pub fn prop_empty_filter() -> impl Strategy<Value = FilterData> {
    Just(FilterData::None)
}

pub fn prop_bit_shuffle_filter() -> impl Strategy<Value = FilterData> {
    Just(FilterData::BitShuffle)
}

pub fn prop_byte_shuffle_filter() -> impl Strategy<Value = FilterData> {
    Just(FilterData::ByteShuffle)
}

pub fn prop_bitwidth_reduction_filter() -> impl Strategy<Value = FilterData> {
    proptest::option::of(0u32..2048)
        .prop_map(|max_window| FilterData::BitWidthReduction { max_window })
}

pub fn prop_checksum_filter() -> impl Strategy<Value = FilterData> {
    prop_oneof![Just(ChecksumType::Md5), Just(ChecksumType::Sha256)]
        .prop_map(FilterData::Checksum)
}

pub fn prop_bzip2_compression_filter() -> impl Strategy<Value = FilterData> {
    proptest::option::of(0..=9).prop_map(|level| {
        FilterData::Compression(CompressionData {
            kind: CompressionType::Bzip2,
            level,
        })
    })
}

pub fn prop_dictionary_compression_filter() -> impl Strategy<Value = FilterData>
{
    Just(FilterData::Compression(CompressionData {
        kind: CompressionType::Dictionary,
        level: None,
    }))
}

pub fn prop_gzip_compression_filter() -> impl Strategy<Value = FilterData> {
    proptest::option::of(0..=9).prop_map(|level| {
        FilterData::Compression(CompressionData {
            kind: CompressionType::Gzip,
            level,
        })
    })
}

pub fn prop_lzr_compression_filter() -> impl Strategy<Value = FilterData> {
    // For whatever reason, TileDB's Lz4 compressor library doesn't use a
    // compression level parameter. Looking at other implementations I can't
    // tell if Lz4 even has a level option at all. Some implementations have
    // one and others don't.
    Just(FilterData::Compression(CompressionData {
        kind: CompressionType::Lz4,
        level: None,
    }))
}

pub fn prop_rle_compression_filter() -> impl Strategy<Value = FilterData> {
    Just(FilterData::Compression(CompressionData {
        kind: CompressionType::Rle,
        level: None,
    }))
}

pub fn prop_zstd_compression_filter() -> impl Strategy<Value = FilterData> {
    proptest::option::of(0..=22).prop_map(|level| {
        FilterData::Compression(CompressionData {
            kind: CompressionType::Zstd,
            level,
        })
    })
}

pub fn prop_delta_compression_filter() -> impl Strategy<Value = FilterData> {
    proptest::option::of(pt_datatype::prop_delta_filter_datatypes()).prop_map(
        |dtype| {
            FilterData::Compression(CompressionData {
                kind: CompressionType::Delta {
                    reinterpret_datatype: dtype,
                },
                level: None,
            })
        },
    )
}

pub fn prop_double_delta_compression_filter(
) -> impl Strategy<Value = FilterData> {
    proptest::option::of(pt_datatype::prop_delta_filter_datatypes()).prop_map(
        |dtype| {
            FilterData::Compression(CompressionData {
                kind: CompressionType::DoubleDelta {
                    reinterpret_datatype: dtype,
                },
                level: None,
            })
        },
    )
}

pub fn prop_compression_filter() -> impl Strategy<Value = FilterData> {
    prop_oneof![
        prop_bzip2_compression_filter(),
        prop_dictionary_compression_filter(),
        prop_gzip_compression_filter(),
        prop_lzr_compression_filter(),
        prop_rle_compression_filter(),
        prop_zstd_compression_filter(),
        prop_delta_compression_filter(),
        prop_double_delta_compression_filter(),
    ]
}

pub fn prop_positive_delta_filter() -> impl Strategy<Value = FilterData> {
    proptest::option::of(0u32..(1024 * 1024))
        .prop_map(|max_window| FilterData::PositiveDelta { max_window })
}

pub fn prop_scale_float_filter() -> impl Strategy<Value = FilterData> {
    let byte_width = proptest::option::of(prop_oneof![
        Just(ScaleFloatByteWidth::I8),
        Just(ScaleFloatByteWidth::I16),
        Just(ScaleFloatByteWidth::I32),
        Just(ScaleFloatByteWidth::I64),
    ]);
    let factor = proptest::option::of(prop_oneof![
        ..-f64::MIN_POSITIVE,
        f64::MIN_POSITIVE..
    ]);
    let offset = proptest::option::of(any::<f64>());
    (byte_width, factor, offset).prop_map(|(byte_width, factor, offset)| {
        FilterData::ScaleFloat {
            byte_width,
            factor,
            offset,
        }
    })
}

pub fn prop_webp_filter() -> impl Strategy<Value = FilterData> {
    let input_format = prop_oneof![
        Just(WebPFilterInputFormat::Rgb),
        Just(WebPFilterInputFormat::Bgr),
        Just(WebPFilterInputFormat::Rgba),
        Just(WebPFilterInputFormat::Bgra),
    ];
    let lossless = proptest::option::of(any::<bool>());
    let quality = proptest::option::of(0.0f32..=100.0);
    (input_format, lossless, quality).prop_map(
        |(input_format, lossless, quality)| FilterData::WebP {
            input_format,
            lossless,
            quality,
        },
    )
}

pub fn prop_xor_filter() -> impl Strategy<Value = FilterData> {
    Just(FilterData::Xor)
}

pub fn prop_filter() -> impl Strategy<Value = FilterData> {
    prop_oneof![
        prop_empty_filter(),
        prop_bit_shuffle_filter(),
        prop_byte_shuffle_filter(),
        prop_bitwidth_reduction_filter(),
        prop_checksum_filter(),
        prop_compression_filter(),
        prop_positive_delta_filter(),
        prop_scale_float_filter(),
        prop_webp_filter(),
        prop_xor_filter(),
    ]
}

pub fn maybe_empty_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    Some(prop_empty_filter().boxed())
}

pub fn maybe_bit_shuffle_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    Some(prop_bit_shuffle_filter().boxed())
}

pub fn maybe_byte_shuffle_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    Some(prop_byte_shuffle_filter().boxed())
}

pub fn maybe_bitwidth_reduction_filter(
    datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    if datatype.is_integral_type()
        || datatype.is_datetime_type()
        || datatype.is_time_type()
        || datatype.is_byte_type()
    {
        Some(prop_bitwidth_reduction_filter().boxed())
    } else {
        None
    }
}

pub fn maybe_checksum_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    Some(prop_checksum_filter().boxed())
}

pub fn maybe_bzip2_compression_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    Some(prop_bzip2_compression_filter().boxed())
}

pub fn maybe_dictionary_compression_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    if index != 0 {
        return None;
    }

    Some(prop_dictionary_compression_filter().boxed())
}

pub fn maybe_gzip_compression_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    Some(prop_gzip_compression_filter().boxed())
}

pub fn maybe_lzr_compression_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    Some(prop_lzr_compression_filter().boxed())
}

pub fn maybe_rle_compression_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    if index != 0 {
        return None;
    }

    Some(prop_rle_compression_filter().boxed())
}

pub fn maybe_zstd_compression_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    Some(prop_zstd_compression_filter().boxed())
}

pub fn maybe_delta_compression_filter(
    datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    if !datatype.is_real_type() {
        Some(prop_delta_compression_filter().boxed())
    } else {
        None
    }
}

pub fn maybe_double_delta_compression_filter(
    datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    if !datatype.is_real_type() {
        Some(prop_double_delta_compression_filter().boxed())
    } else {
        None
    }
}

pub fn maybe_compression_filter(
    datatype: Datatype,
    cell_val_num: CellValNum,
    index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    let possible: Vec<BoxedStrategy<FilterData>> = vec![
        maybe_bzip2_compression_filter(datatype, cell_val_num, index),
        maybe_dictionary_compression_filter(datatype, cell_val_num, index),
        maybe_gzip_compression_filter(datatype, cell_val_num, index),
        maybe_lzr_compression_filter(datatype, cell_val_num, index),
        maybe_rle_compression_filter(datatype, cell_val_num, index),
        maybe_zstd_compression_filter(datatype, cell_val_num, index),
        maybe_delta_compression_filter(datatype, cell_val_num, index),
        maybe_double_delta_compression_filter(datatype, cell_val_num, index),
    ]
    .into_iter()
    .filter(Option::is_some)
    .flatten()
    .collect();

    Some(proptest::strategy::Union::new(possible).boxed())
}

pub fn maybe_positive_delta_filter(
    datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    if datatype.is_integral_type()
        || datatype.is_datetime_type()
        || datatype.is_time_type()
        || datatype.is_byte_type()
    {
        Some(prop_positive_delta_filter().boxed())
    } else {
        None
    }
}

pub fn maybe_scale_float_filter(
    datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    let input_size = datatype.size() as usize;
    if input_size == std::mem::size_of::<f32>()
        || input_size == std::mem::size_of::<f64>()
    {
        Some(prop_scale_float_filter().boxed())
    } else {
        None
    }
}

pub fn maybe_webp_filter(
    datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    if matches!(datatype, Datatype::UInt8) {
        Some(prop_webp_filter().boxed())
    } else {
        None
    }
}

pub fn maybe_xor_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<BoxedStrategy<FilterData>> {
    Some(prop_xor_filter().boxed())
}

pub fn prop_filter_data_with_constraints(
    datatype: Datatype,
    cell_val_num: CellValNum,
    index: usize,
) -> BoxedStrategy<FilterData> {
    let possible: Vec<BoxedStrategy<FilterData>> = vec![
        maybe_empty_filter(datatype, cell_val_num, index),
        maybe_bit_shuffle_filter(datatype, cell_val_num, index),
        maybe_byte_shuffle_filter(datatype, cell_val_num, index),
        maybe_bitwidth_reduction_filter(datatype, cell_val_num, index),
        maybe_checksum_filter(datatype, cell_val_num, index),
        maybe_compression_filter(datatype, cell_val_num, index),
        maybe_positive_delta_filter(datatype, cell_val_num, index),
        maybe_scale_float_filter(datatype, cell_val_num, index),
        maybe_webp_filter(datatype, cell_val_num, index),
        maybe_xor_filter(datatype, cell_val_num, index),
    ]
    .into_iter()
    .filter(Option::is_some)
    .flatten()
    .collect();

    assert!(!possible.is_empty());

    proptest::strategy::Union::new(possible).boxed()
}
