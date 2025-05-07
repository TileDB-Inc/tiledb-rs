#ifndef TILEDB_RS_API_WEBP_FORMAT_H
#define TILEDB_RS_API_WEBP_FORMAT_H

#include <tiledb/tiledb.h>

#include "tiledb-sys2/src/webp_format.rs.h"

namespace tiledb::rs {

tiledb_filter_webp_format_t to_cpp_webp_format(WebPFormat ft);
WebPFormat to_rs_webp_format(tiledb_filter_webp_format_t ft);

}  // namespace tiledb::rs

#endif
