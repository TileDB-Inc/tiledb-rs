#ifndef TILEDB_RS_API_ARRAY_TYPE_H
#define TILEDB_RS_API_ARRAY_TYPE_H

#include <tiledb/tiledb.h>

#include "tiledb-sys2/src/array_type.rs.h"

namespace tiledb::rs {

tiledb_array_type_t to_cpp_array_type(ArrayType at);
ArrayType to_rs_array_type(tiledb_array_type_t at);

}  // namespace tiledb::rs

#endif
