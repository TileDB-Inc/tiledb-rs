#include <string>

#include <tiledb/tiledb.h>

#include "exception.h"
#include "tiledb-sys2/src/mode.rs.h"

namespace tiledb::rs {

tiledb_query_type_t to_cpp_mode(Mode mode) {
  switch (mode) {
    case Mode::Read:
      return TILEDB_READ;
    case Mode::Write:
      return TILEDB_WRITE;
    case Mode::Delete:
      return TILEDB_DELETE;
    case Mode::Update:
      return TILEDB_UPDATE;
    case Mode::ModifyExclusive:
      return TILEDB_MODIFY_EXCLUSIVE;
    default:
      throw TileDBError("Invalid TileOrder for conversion.");
  }
}

Mode to_rs_mode(tiledb_query_type_t mode) {
  switch (mode) {
    case TILEDB_READ:
      return Mode::Read;
    case TILEDB_WRITE:
      return Mode::Write;
    case TILEDB_DELETE:
      return Mode::Delete;
    case TILEDB_UPDATE:
      return Mode::Update;
    case TILEDB_MODIFY_EXCLUSIVE:
      return Mode::ModifyExclusive;
    default:
      throw TileDBError("Invalid tiledb_layout_t for TileOrder conversion.");
  }
}

}  // namespace tiledb::rs
