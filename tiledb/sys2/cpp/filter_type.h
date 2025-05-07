#ifndef TILEDB_RS_API_FILTER_TYPE_H
#define TILEDB_RS_API_FILTER_TYPE_H

#include <tiledb/tiledb.h>

#include "tiledb-sys2/src/filter_type.rs.h"

namespace tiledb::rs {

tiledb_filter_type_t to_cpp_filter_type(FilterType ft);
FilterType to_rs_filter_type(tiledb_filter_type_t ft);

}  // namespace tiledb::rs

#endif
