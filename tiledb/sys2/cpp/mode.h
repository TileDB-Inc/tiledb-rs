#ifndef TILEDB_RS_API_MODE_H
#define TILEDB_RS_API_MODE_H

#include <tiledb/tiledb.h>

#include "tiledb-sys2/src/mode.rs.h"

namespace tiledb::rs {

tiledb_query_type_t to_cpp_mode(Mode mode);
Mode to_rs_mode(tiledb_query_type_t mode);

}  // namespace tiledb::rs

#endif
