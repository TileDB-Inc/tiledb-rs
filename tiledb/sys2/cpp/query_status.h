#ifndef TILEDB_RS_API_QUERY_STATUS_H
#define TILEDB_RS_API_QUERY_STATUS_H

#include <tiledb/tiledb.h>

#include "tiledb-sys2/src/query_status.rs.h"

namespace tiledb::rs {

tiledb_query_status_t to_cpp_query_status(QueryStatus status);
QueryStatus to_rs_query_status(tiledb_query_status_t status);

}  // namespace tiledb::rs

#endif
