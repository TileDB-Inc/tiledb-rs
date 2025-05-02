
#ifndef TILEDB_RS_API_EXCEPTION_H
#define TILEDB_RS_API_EXCEPTION_H

#include <stdexcept>

namespace tiledb::rs {

/** Exception indicating a TileDB error. */
struct TileDBError : std::runtime_error {
  TileDBError(const std::string& msg)
      : std::runtime_error(msg) {
  }
};

}  // namespace tiledb::rs

#endif  // TILEDB_CPP_API_EXCEPTION_H
