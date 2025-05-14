#ifndef TILEDB_RS_API_LAYOUT_H
#define TILEDB_RS_API_LAYOUT_H

#include <tiledb/tiledb.h>

#include "tiledb-sys2/src/layout.rs.h"

namespace tiledb::rs {

tiledb_layout_t to_cpp_tile_order(TileOrder order);
TileOrder to_rs_tile_order(tiledb_layout_t layout);

tiledb_layout_t to_cpp_cell_order(CellOrder order);
CellOrder to_rs_cell_order(tiledb_layout_t layout);

}  // namespace tiledb::rs

#endif
