#include <string>

#include <tiledb/tiledb.h>

#include "exception.h"

#include "tiledb-sys2/src/webp_format.rs.h"

namespace tiledb::rs {

tiledb_filter_webp_format_t to_cpp_webp_format(WebPFormat wf) {
  switch (wf) {
    case WebPFormat::None:
      return TILEDB_WEBP_NONE;
    case WebPFormat::Rgb:
      return TILEDB_WEBP_RGB;
    case WebPFormat::Bgr:
      return TILEDB_WEBP_BGR;
    case WebPFormat::Rgba:
      return TILEDB_WEBP_RGBA;
    case WebPFormat::Bgra:
      return TILEDB_WEBP_BGRA;
    default:
      throw TileDBError("Invalid WebPFilter for conversion.");
  }
}

WebPFormat to_rs_webp_format(tiledb_filter_webp_format_t wf) {
  switch (wf) {
    case TILEDB_WEBP_NONE:
      return WebPFormat::None;
    case TILEDB_WEBP_RGB:
      return WebPFormat::Rgb;
    case TILEDB_WEBP_BGR:
      return WebPFormat::Bgr;
    case TILEDB_WEBP_RGBA:
      return WebPFormat::Rgba;
    case TILEDB_WEBP_BGRA:
      return WebPFormat::Bgra;
    default:
      throw TileDBError("Invalid tiledb_filter_webp_format_t for conversion.");
  }
}

}  // namespace tiledb::rs
