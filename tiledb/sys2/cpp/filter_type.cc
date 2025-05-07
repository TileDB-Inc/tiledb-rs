#include <string>

#include <tiledb/tiledb.h>

#include "exception.h"

#include "tiledb-sys2/src/filter_type.rs.h"

namespace tiledb::rs {

tiledb_filter_type_t to_cpp_datatype(FilterType ft) {
  switch (ft) {
    case FilterType::None:
      return TILEDB_FILTER_NONE;
    case FilterType::Gzip:
      return TILEDB_FILTER_GZIP;
    case FilterType::Zstd:
      return TILEDB_FILTER_ZSTD;
    case FilterType::Lz4:
      return TILEDB_FILTER_LZ4;
    case FilterType::Rle:
      return TILEDB_FILTER_RLE;
    case FilterType::Bzip2:
      return TILEDB_FILTER_BZIP2;
    case FilterType::DoubleDelta:
      return TILEDB_FILTER_DOUBLE_DELTA;
    case FilterType::BitWidthReduction:
      return TILEDB_FILTER_BIT_WIDTH_REDUCTION;
    case FilterType::BitShuffle:
      return TILEDB_FILTER_BITSHUFFLE;
    case FilterType::ByteShuffle:
      return TILEDB_FILTER_BYTESHUFFLE;
    case FilterType::PositiveDelta:
      return TILEDB_FILTER_POSITIVE_DELTA;
    case FilterType::ChecksumMd5:
      return TILEDB_FILTER_CHECKSUM_MD5;
    case FilterType::ChecksumSha256:
      return TILEDB_FILTER_CHECKSUM_SHA256;
    case FilterType::Dictionary:
      return TILEDB_FILTER_DICTIONARY;
    case FilterType::ScaleFloat:
      return TILEDB_FILTER_SCALE_FLOAT;
    case FilterType::Xor:
      return TILEDB_FILTER_XOR;
    case FilterType::Deprecated:
      return TILEDB_FILTER_DEPRECATED;
    case FilterType::WebP:
      return TILEDB_FILTER_WEBP;
    case FilterType::Delta:
      return TILEDB_FILTER_DELTA;
    default:
      throw TileDBError("Invalid FilterType for conversion.");
  }
}

FilterType to_rs_filter_type(tiledb_filter_type_t ft) {
  switch (ft) {
    case TILEDB_FILTER_NONE:
      return FilterType::None;
    case TILEDB_FILTER_GZIP:
      return FilterType::Gzip;
    case TILEDB_FILTER_ZSTD:
      return FilterType::Zstd;
    case TILEDB_FILTER_LZ4:
      return FilterType::Lz4;
    case TILEDB_FILTER_RLE:
      return FilterType::Rle;
    case TILEDB_FILTER_BZIP2:
      return FilterType::Bzip2;
    case TILEDB_FILTER_DOUBLE_DELTA:
      return FilterType::DoubleDelta;
    case TILEDB_FILTER_BIT_WIDTH_REDUCTION:
      return FilterType::BitWidthReduction;
    case TILEDB_FILTER_BITSHUFFLE:
      return FilterType::BitShuffle;
    case TILEDB_FILTER_BYTESHUFFLE:
      return FilterType::ByteShuffle;
    case TILEDB_FILTER_POSITIVE_DELTA:
      return FilterType::PositiveDelta;
    case TILEDB_FILTER_CHECKSUM_MD5:
      return FilterType::ChecksumMd5;
    case TILEDB_FILTER_CHECKSUM_SHA256:
      return FilterType::ChecksumSha256;
    case TILEDB_FILTER_DICTIONARY:
      return FilterType::Dictionary;
    case TILEDB_FILTER_SCALE_FLOAT:
      return FilterType::ScaleFloat;
    case TILEDB_FILTER_XOR:
      return FilterType::Xor;
    case TILEDB_FILTER_DEPRECATED:
      return FilterType::Deprecated;
    case TILEDB_FILTER_WEBP:
      return FilterType::WebP;
    case TILEDB_FILTER_DELTA:
      return FilterType::Delta;
    default:
      throw TileDBError("Invalid tiledb_filter_type_t for conversion.");
  }
}

}  // namespace tiledb::rs
