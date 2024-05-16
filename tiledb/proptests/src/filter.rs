use proptest::test_runner::TestRng;
use rand::distributions::Uniform;
use rand::Rng;

use tiledb::array::schema::{CellValNum, SchemaData};
use tiledb::array::ArrayType;
use tiledb::datatype::Datatype;
use tiledb::filter::{
    ChecksumType, CompressionData, CompressionType, FilterData,
    ScaleFloatByteWidth, WebPFilterInputFormat,
};

use crate::datatype;
use crate::filter_list::FilterListContextKind;
use crate::util;

type FilterGenerator = fn(&mut TestRng) -> FilterData;

pub struct FilterListContext<'a> {
    kind: FilterListContextKind,
    schema: &'a SchemaData,
    field_datatype: Datatype,
    datatype: Datatype,
    cell_val_num: CellValNum,
    index: usize,
}

impl<'a> FilterListContext<'a> {
    pub fn new(
        kind: FilterListContextKind,
        schema: &'a SchemaData,
        field_datatype: Datatype,
        datatype: Datatype,
        cell_val_num: CellValNum,
        index: usize,
    ) -> Self {
        Self {
            kind,
            schema,
            field_datatype,
            datatype,
            cell_val_num,
            index,
        }
    }
}

fn gen_empty_filter(_rng: &mut TestRng) -> FilterData {
    FilterData::None
}

fn gen_bit_shuffle_filter(_rng: &mut TestRng) -> FilterData {
    FilterData::BitShuffle
}

fn gen_byte_shuffle_filter(_rng: &mut TestRng) -> FilterData {
    FilterData::ByteShuffle
}

fn gen_bitwidth_reduction_filter(rng: &mut TestRng) -> FilterData {
    let max_window = if rng.gen_bool(0.5) {
        Some(rng.gen_range(0u32..2048))
    } else {
        None
    };
    FilterData::BitWidthReduction { max_window }
}

fn gen_checksum_filter(rng: &mut TestRng) -> FilterData {
    if rng.gen_bool(0.5) {
        FilterData::Checksum(ChecksumType::Md5)
    } else {
        FilterData::Checksum(ChecksumType::Sha256)
    }
}

fn gen_bzip2_compression_filter(rng: &mut TestRng) -> FilterData {
    let level = if rng.gen_bool(0.5) {
        Some(rng.gen_range(0..=9))
    } else {
        None
    };
    FilterData::Compression(CompressionData {
        kind: CompressionType::Bzip2,
        level,
    })
}

fn gen_dictionary_compression_filter(_rng: &mut TestRng) -> FilterData {
    FilterData::Compression(CompressionData {
        kind: CompressionType::Dictionary,
        level: None,
    })
}

fn gen_gzip_compression_filter(rng: &mut TestRng) -> FilterData {
    let level = if rng.gen_bool(0.5) {
        Some(rng.gen_range(0..=9))
    } else {
        None
    };
    FilterData::Compression(CompressionData {
        kind: CompressionType::Gzip,
        level,
    })
}

fn gen_lz4_compression_filter(_rng: &mut TestRng) -> FilterData {
    // For whatever reason, TileDB's Lz4 compressor library doesn't use a
    // compression level parameter. Looking at other implementations I can't
    // tell if Lz4 even has a level option at all. Some implementations have
    // one and others don't.
    FilterData::Compression(CompressionData {
        kind: CompressionType::Lz4,
        level: None,
    })
}

fn gen_rle_compression_filter(_rng: &mut TestRng) -> FilterData {
    FilterData::Compression(CompressionData {
        kind: CompressionType::Rle,
        level: None,
    })
}

fn gen_zstd_compression_filter(rng: &mut TestRng) -> FilterData {
    let level = if rng.gen_bool(0.5) {
        Some(rng.gen_range(0..=22))
    } else {
        None
    };
    FilterData::Compression(CompressionData {
        kind: CompressionType::Zstd,
        level,
    })
}

fn gen_delta_compression_filter(rng: &mut TestRng) -> FilterData {
    let rtype = if rng.gen_bool(0.5) {
        Some(util::choose(rng, &datatype::delta_datatypes_vec()))
    } else {
        None
    };
    FilterData::Compression(CompressionData {
        kind: CompressionType::Delta {
            reinterpret_datatype: rtype,
        },
        level: None,
    })
}

fn gen_double_delta_compression_filter(rng: &mut TestRng) -> FilterData {
    let rtype = if rng.gen_bool(0.5) {
        Some(util::choose(rng, &datatype::delta_datatypes_vec()))
    } else {
        None
    };
    FilterData::Compression(CompressionData {
        kind: CompressionType::Delta {
            reinterpret_datatype: rtype,
        },
        level: None,
    })
}

fn gen_positive_delta_filter(rng: &mut TestRng) -> FilterData {
    let max_window = if rng.gen_bool(0.5) {
        Some(rng.gen_range(0u32..(1024 * 1024)))
    } else {
        None
    };
    FilterData::PositiveDelta { max_window }
}

fn gen_scale_float_filter(rng: &mut TestRng) -> FilterData {
    let byte_width = if rng.gen_bool(0.5) {
        Some(util::choose(
            rng,
            &[
                ScaleFloatByteWidth::I8,
                ScaleFloatByteWidth::I16,
                ScaleFloatByteWidth::I32,
                ScaleFloatByteWidth::I64,
            ],
        ))
    } else {
        None
    };

    let factor = if rng.gen_bool(0.5) {
        if rng.gen_bool(0.5) {
            Some(rng.sample(Uniform::new_inclusive(
                f64::MIN_POSITIVE,
                1000000.0f64,
            )))
        } else {
            Some(rng.sample(Uniform::new_inclusive(
                -1000000.0f64,
                -f64::MIN_POSITIVE,
            )))
        }
    } else {
        None
    };

    let offset = if rng.gen_bool(0.5) {
        Some(rng.sample(Uniform::new_inclusive(-1000000.0f64, 1000000.0f64)))
    } else {
        None
    };

    FilterData::ScaleFloat {
        byte_width,
        factor,
        offset,
    }
}

fn gen_webp_filter(
    rng: &mut TestRng,
    formats: &[WebPFilterInputFormat],
) -> FilterData {
    let input_format = util::choose(rng, formats);

    let lossless = if rng.gen_bool(0.5) {
        Some(rng.gen_bool(0.5))
    } else {
        None
    };

    let quality = if rng.gen_bool(0.5) {
        Some(rng.sample(Uniform::new_inclusive(0.0f32, 100.0f32)))
    } else {
        None
    };

    FilterData::WebP {
        input_format,
        lossless,
        quality,
    }
}

fn gen_webp_filter_three_channel(rng: &mut TestRng) -> FilterData {
    gen_webp_filter(
        rng,
        &[WebPFilterInputFormat::Rgb, WebPFilterInputFormat::Bgr],
    )
}

fn gen_webp_filter_four_channel(rng: &mut TestRng) -> FilterData {
    gen_webp_filter(
        rng,
        &[WebPFilterInputFormat::Rgba, WebPFilterInputFormat::Bgra],
    )
}

fn gen_xor_filter(_rng: &mut TestRng) -> FilterData {
    FilterData::Xor
}

fn maybe_empty_filter(_ctx: &FilterListContext) -> Option<FilterGenerator> {
    Some(gen_empty_filter)
}

fn maybe_bit_shuffle_filter(
    _ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    Some(gen_bit_shuffle_filter)
}

fn maybe_byte_shuffle_filter(
    _ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    Some(gen_byte_shuffle_filter)
}

fn maybe_bitwidth_reduction_filter(
    ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    if ctx.datatype.is_integral_type()
        || ctx.datatype.is_datetime_type()
        || ctx.datatype.is_time_type()
        || ctx.datatype.is_byte_type()
    {
        Some(gen_bitwidth_reduction_filter)
    } else {
        None
    }
}

fn maybe_checksum_filter(_ctx: &FilterListContext) -> Option<FilterGenerator> {
    Some(gen_checksum_filter)
}

fn maybe_bzip2_compression_filter(
    _ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    Some(gen_bzip2_compression_filter)
}

fn maybe_dictionary_compression_filter(
    ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    if ctx.index != 0 {
        return None;
    }

    Some(gen_dictionary_compression_filter)
}

fn maybe_gzip_compression_filter(
    _ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    Some(gen_gzip_compression_filter)
}

fn maybe_lz4_compression_filter(
    _ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    Some(gen_lz4_compression_filter)
}

fn maybe_rle_compression_filter(
    ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    if ctx.index != 0 {
        return None;
    }

    Some(gen_rle_compression_filter)
}

fn maybe_zstd_compression_filter(
    _ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    Some(gen_zstd_compression_filter)
}

fn maybe_delta_compression_filter(
    ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    if !ctx.datatype.is_real_type() {
        Some(gen_delta_compression_filter)
    } else {
        None
    }
}

fn maybe_double_delta_compression_filter(
    ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    if !ctx.datatype.is_real_type() {
        Some(gen_double_delta_compression_filter)
    } else {
        None
    }
}

fn maybe_positive_delta_filter(
    ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    if ctx.datatype.is_integral_type()
        || ctx.datatype.is_datetime_type()
        || ctx.datatype.is_time_type()
        || ctx.datatype.is_byte_type()
    {
        Some(gen_positive_delta_filter)
    } else {
        None
    }
}

fn maybe_scale_float_filter(
    ctx: &FilterListContext,
) -> Option<FilterGenerator> {
    let input_size = ctx.datatype.size() as usize;
    if input_size == std::mem::size_of::<f32>()
        || input_size == std::mem::size_of::<f64>()
    {
        Some(gen_scale_float_filter)
    } else {
        None
    }
}

fn maybe_webp_filter(ctx: &FilterListContext) -> Option<FilterGenerator> {
    // WebP requires a dense array
    if !matches!(ctx.schema.array_type, ArrayType::Dense) {
        return None;
    }

    // WebP requires a 2-dimensional array
    if ctx.schema.domain.dimension.len() != 2 {
        return None;
    }

    // Only attributes are supported
    if !matches!(ctx.kind, FilterListContextKind::Attribute) {
        return None;
    }

    // From schema check, the underlying attribute must be UInt8
    if !matches!(ctx.field_datatype, Datatype::UInt8) {
        return None;
    }

    // Don't apply to an attribute filter list that's already changed the
    // datataype.
    if !matches!(ctx.datatype, Datatype::UInt8) {
        return None;
    }

    // Colorspace requirements means we need a CellVallNum of 3 or 4
    let cvn = u32::from(ctx.cell_val_num);
    if cvn != 3 && cvn != 4 {
        return None;
    }

    let extent = ctx.schema.domain.dimension[1].extent.as_ref()?;
    let extent = serde_json::value::from_value::<usize>(extent.clone());
    if extent.is_err() {
        return None;
    }
    let extent = extent.unwrap();

    if cvn as usize != extent {
        return None;
    }

    if cvn == 3 {
        return Some(gen_webp_filter_four_channel);
    } else if cvn == 4 {
        return Some(gen_webp_filter_four_channel);
    }

    unreachable!();
}

fn maybe_xor_filter(_ctx: &FilterListContext) -> Option<FilterGenerator> {
    Some(gen_xor_filter)
}

pub fn generate_with_constraints(
    rng: &mut TestRng,
    kind: FilterListContextKind,
    schema: &SchemaData,
    field_datatype: Datatype,
    datatype: Datatype,
    cell_val_num: CellValNum,
    index: usize,
) -> FilterData {
    let ctx = FilterListContext::new(
        kind,
        schema,
        field_datatype,
        datatype,
        cell_val_num,
        index,
    );

    let possible: Vec<FilterGenerator> = vec![
        maybe_empty_filter(&ctx),
        maybe_bit_shuffle_filter(&ctx),
        maybe_byte_shuffle_filter(&ctx),
        maybe_bitwidth_reduction_filter(&ctx),
        maybe_checksum_filter(&ctx),
        maybe_bzip2_compression_filter(&ctx),
        maybe_dictionary_compression_filter(&ctx),
        maybe_gzip_compression_filter(&ctx),
        maybe_lz4_compression_filter(&ctx),
        maybe_rle_compression_filter(&ctx),
        maybe_zstd_compression_filter(&ctx),
        maybe_delta_compression_filter(&ctx),
        maybe_double_delta_compression_filter(&ctx),
        maybe_positive_delta_filter(&ctx),
        maybe_scale_float_filter(&ctx),
        maybe_webp_filter(&ctx),
        maybe_xor_filter(&ctx),
    ]
    .into_iter()
    .filter(Option::is_some)
    .flatten()
    .collect();

    assert!(!possible.is_empty());

    let dist = Uniform::new(0, possible.len());
    let idx = rng.sample(dist);
    let data = possible[idx];
    data(rng)
}
