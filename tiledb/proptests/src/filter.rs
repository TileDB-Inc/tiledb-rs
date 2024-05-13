use proptest::test_runner::{TestRng, TestRunner};
use rand::distributions::Uniform;
use rand::Rng;

use tiledb::array::schema::CellValNum;
use tiledb::datatype::Datatype;
use tiledb::filter::{
    ChecksumType, CompressionData, CompressionType, FilterData,
    ScaleFloatByteWidth, WebPFilterInputFormat,
};

use crate::datatype as pt_datatype;
use crate::util as pt_util;

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
        Some(pt_util::choose(rng, &pt_datatype::delta_datatypes_vec()))
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
        Some(pt_util::choose(rng, &pt_datatype::delta_datatypes_vec()))
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
        Some(pt_util::choose(
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

fn gen_webp_filter(rng: &mut TestRng) -> FilterData {
    let input_format = pt_util::choose(
        rng,
        &[
            WebPFilterInputFormat::Rgb,
            WebPFilterInputFormat::Bgr,
            WebPFilterInputFormat::Rgba,
            WebPFilterInputFormat::Bgra,
        ],
    );

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

fn gen_xor_filter(_rng: &mut TestRng) -> FilterData {
    FilterData::Xor
}

type FilterGenerator = fn(&mut TestRng) -> FilterData;

pub fn maybe_empty_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    Some(gen_empty_filter)
}

pub fn maybe_bit_shuffle_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    Some(gen_bit_shuffle_filter)
}

pub fn maybe_byte_shuffle_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    Some(gen_byte_shuffle_filter)
}

pub fn maybe_bitwidth_reduction_filter(
    datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    if datatype.is_integral_type()
        || datatype.is_datetime_type()
        || datatype.is_time_type()
        || datatype.is_byte_type()
    {
        Some(gen_bitwidth_reduction_filter)
    } else {
        None
    }
}

pub fn maybe_checksum_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    Some(gen_checksum_filter)
}

pub fn maybe_bzip2_compression_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    Some(gen_bzip2_compression_filter)
}

pub fn maybe_dictionary_compression_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    index: usize,
) -> Option<FilterGenerator> {
    if index != 0 {
        return None;
    }

    Some(gen_dictionary_compression_filter)
}

pub fn maybe_gzip_compression_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    Some(gen_gzip_compression_filter)
}

pub fn maybe_lz4_compression_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    Some(gen_lz4_compression_filter)
}

pub fn maybe_rle_compression_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    index: usize,
) -> Option<FilterGenerator> {
    if index != 0 {
        return None;
    }

    Some(gen_rle_compression_filter)
}

pub fn maybe_zstd_compression_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    Some(gen_zstd_compression_filter)
}

pub fn maybe_delta_compression_filter(
    datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    if !datatype.is_real_type() {
        Some(gen_delta_compression_filter)
    } else {
        None
    }
}

pub fn maybe_double_delta_compression_filter(
    datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    if !datatype.is_real_type() {
        Some(gen_double_delta_compression_filter)
    } else {
        None
    }
}

pub fn maybe_positive_delta_filter(
    datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    if datatype.is_integral_type()
        || datatype.is_datetime_type()
        || datatype.is_time_type()
        || datatype.is_byte_type()
    {
        Some(gen_positive_delta_filter)
    } else {
        None
    }
}

pub fn maybe_scale_float_filter(
    datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    let input_size = datatype.size() as usize;
    if input_size == std::mem::size_of::<f32>()
        || input_size == std::mem::size_of::<f64>()
    {
        Some(gen_scale_float_filter)
    } else {
        None
    }
}

pub fn maybe_webp_filter(
    datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    if matches!(datatype, Datatype::UInt8) {
        Some(gen_webp_filter)
    } else {
        None
    }
}

pub fn maybe_xor_filter(
    _datatype: Datatype,
    _cell_val_num: CellValNum,
    _index: usize,
) -> Option<FilterGenerator> {
    Some(gen_xor_filter)
}

pub fn generate_with_constraints(
    runner: &mut TestRunner,
    datatype: Datatype,
    cell_val_num: CellValNum,
    index: usize,
) -> FilterData {
    let possible: Vec<FilterGenerator> = vec![
        maybe_empty_filter(datatype, cell_val_num, index),
        maybe_bit_shuffle_filter(datatype, cell_val_num, index),
        maybe_byte_shuffle_filter(datatype, cell_val_num, index),
        maybe_bitwidth_reduction_filter(datatype, cell_val_num, index),
        maybe_checksum_filter(datatype, cell_val_num, index),
        maybe_bzip2_compression_filter(datatype, cell_val_num, index),
        maybe_dictionary_compression_filter(datatype, cell_val_num, index),
        maybe_gzip_compression_filter(datatype, cell_val_num, index),
        maybe_lz4_compression_filter(datatype, cell_val_num, index),
        maybe_rle_compression_filter(datatype, cell_val_num, index),
        maybe_zstd_compression_filter(datatype, cell_val_num, index),
        maybe_delta_compression_filter(datatype, cell_val_num, index),
        maybe_double_delta_compression_filter(datatype, cell_val_num, index),
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

    let dist = Uniform::new(0, possible.len());
    let idx = runner.rng().sample(dist);
    let data = possible[idx];
    data(runner.rng())
}
