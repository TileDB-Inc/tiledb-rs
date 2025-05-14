#ifndef TILEDB_RS_API_DIMENSION_H
#define TILEDB_RS_API_DIMENSION_H

#include <tiledb/tiledb.h>

#include "rust/cxx.h"
#include "tiledb-sys2/src/buffer.rs.h"
#include "tiledb-sys2/src/datatype.rs.h"

namespace tiledb::rs {

class Context;
class FilterList;

class Dimension {
 public:
  Dimension(std::shared_ptr<Context> ctx, tiledb_dimension_t* dim);
  Dimension(
      std::shared_ptr<Context> ctx, std::shared_ptr<tiledb_dimension_t> dim);

  rust::String name() const;
  Datatype datatype() const;
  bool domain(Buffer& value) const;
  bool tile_extent(Buffer& value) const;
  uint32_t cell_val_num() const;
  std::shared_ptr<FilterList> filter_list() const;

  std::shared_ptr<tiledb_dimension_t> ptr() const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_dimension_t> dim_;
};

class DimensionBuilder {
 public:
  DimensionBuilder(
      std::shared_ptr<Context> ctx,
      rust::Str name,
      Datatype type,
      Buffer& domain,
      Buffer& extent);

  std::shared_ptr<Dimension> build() const;

  void set_cell_val_num(uint32_t num) const;
  void set_filter_list(std::shared_ptr<FilterList>) const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_dimension_t> dim_;
};

std::shared_ptr<DimensionBuilder> create_dimension_builder(
    std::shared_ptr<Context> ctx,
    rust::Str name,
    Datatype dtype,
    Buffer& domain,
    Buffer& extent);

}  // namespace tiledb::rs

#endif
