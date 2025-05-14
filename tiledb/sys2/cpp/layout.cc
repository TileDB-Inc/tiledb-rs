#include <string>

#include <tiledb/tiledb.h>

#include "exception.h"
#include "tiledb-sys2/src/layout.rs.h"

namespace tiledb::rs {

tiledb_layout_t to_cpp_tile_order(TileOrder order) {
  switch (order) {
    case TileOrder::RowMajor:
      return TILEDB_ROW_MAJOR;
    case TileOrder::ColumnMajor:
      return TILEDB_COL_MAJOR;
    default:
      throw TileDBError("Invalid TileOrder for conversion.");
  }
}

TileOrder to_rs_tile_order(tiledb_layout_t order) {
  switch (order) {
    case TILEDB_ROW_MAJOR:
      return TileOrder::RowMajor;
    case TILEDB_COL_MAJOR:
      return TileOrder::ColumnMajor;
    default:
      throw TileDBError("Invalid tiledb_layout_t for TileOrder conversion.");
  }
}

tiledb_layout_t to_cpp_cell_order(CellOrder order) {
  switch (order) {
    case CellOrder::Unordered:
      return TILEDB_UNORDERED;
    case CellOrder::RowMajor:
      return TILEDB_ROW_MAJOR;
    case CellOrder::ColumnMajor:
      return TILEDB_COL_MAJOR;
    case CellOrder::Global:
      return TILEDB_GLOBAL_ORDER;
    case CellOrder::Hilbert:
      return TILEDB_HILBERT;
    default:
      throw TileDBError("Invalid CellOrder for conversion.");
  }
}

CellOrder to_rs_cell_order(tiledb_layout_t order) {
  switch (order) {
    case TILEDB_UNORDERED:
      return CellOrder::Unordered;
    case TILEDB_ROW_MAJOR:
      return CellOrder::RowMajor;
    case TILEDB_COL_MAJOR:
      return CellOrder::ColumnMajor;
    case TILEDB_GLOBAL_ORDER:
      return CellOrder::Global;
    case TILEDB_HILBERT:
      return CellOrder::Hilbert;
    default:
      throw TileDBError("Invalid tiledb_layout_t for CellOrder conversion.");
  }
}

}  // namespace tiledb::rs
