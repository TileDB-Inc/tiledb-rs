#include <string>

#include <tiledb/tiledb.h>

#include "exception.h"
#include "tiledb-sys2/src/array_type.rs.h"

namespace tiledb::rs {

tiledb_array_type_t to_cpp_array_type(ArrayType dt) {
  switch (dt) {
    case ArrayType::Dense:
      return TILEDB_DENSE;
    case ArrayType::Sparse:
      return TILEDB_SPARSE;
    default:
      throw TileDBError("Invalid ArrayType for conversion.");
  }
}

ArrayType to_rs_array_type(tiledb_array_type_t at) {
  switch (at) {
    case TILEDB_DENSE:
      return ArrayType::Dense;
    case TILEDB_SPARSE:
      return ArrayType::Sparse;
    default:
      throw TileDBError("Invalid tiledb_datatype_t for conversion.");
  }
}

}  // namespace tiledb::rs
