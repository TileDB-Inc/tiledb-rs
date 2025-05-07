#ifndef TILEDB_RS_API_DATATYPE_H
#define TILEDB_RS_API_DATATYPE_H

#include <tiledb/tiledb.h>

#include "tiledb-sys2/src/datatype.rs.h"

namespace tiledb::rs {

tiledb_datatype_t to_cpp_datatype(Datatype dt);
Datatype to_rs_datatype(tiledb_datatype_t dt);

}  // namespace tiledb::rs

#endif
