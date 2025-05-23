#include <string>

#include <tiledb/tiledb.h>

#include "exception.h"
#include "tiledb-sys2/src/query_status.rs.h"

namespace tiledb::rs {

tiledb_query_status_t to_cpp_query_status(QueryStatus status) {
  switch (status) {
    case QueryStatus::Failed:
      return TILEDB_FAILED;
    case QueryStatus::Completed:
      return TILEDB_COMPLETED;
    case QueryStatus::InProgress:
      return TILEDB_INPROGRESS;
    case QueryStatus::Incomplete:
      return TILEDB_INCOMPLETE;
    case QueryStatus::Uninitialized:
      return TILEDB_UNINITIALIZED;
    case QueryStatus::Initialized:
      return TILEDB_INITIALIZED;
    default:
      throw TileDBError("Invalid TileOrder for conversion.");
  }
}

QueryStatus to_rs_query_status(tiledb_query_status_t status) {
  switch (status) {
    case TILEDB_FAILED:
      return QueryStatus::Failed;
    case TILEDB_COMPLETED:
      return QueryStatus::Completed;
    case TILEDB_INPROGRESS:
      return QueryStatus::InProgress;
    case TILEDB_INCOMPLETE:
      return QueryStatus::Incomplete;
    case TILEDB_UNINITIALIZED:
      return QueryStatus::Uninitialized;
    case TILEDB_INITIALIZED:
      return QueryStatus::Initialized;
    default:
      throw TileDBError("Invalid tiledb_layout_t for TileOrder conversion.");
  }
}

}  // namespace tiledb::rs
