#ifndef TILEDB_RS_API_DIMENSION_H
#define TILEDB_RS_API_DIMENSION_H

#include <string>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Context;
class FilterList;

class Dimension {
 public:
  Dimension(const Context& ctx, tiledb_dimension_t* dim);

  const std::string name() const;
  tiledb_datatype_t type() const;
  uint32_t cell_val_num() const;
  FilterList filter_list() const;

  template <typename T>
  std::pair<T, T> domain() const;

  template <typename T>
  T tile_extent() const;

  std::shared_ptr<tiledb_dimension_t> ptr() const {
    return dim_;
  }

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_dimension_t> dim_;
};

class DimensionBuilder {
 public:
  DimensionBuilder(
      std::shared_ptr<const Context> ctx,
      std::string name,
      tiledb_datatype_t type,
      const void* domain,
      const void* tile_extent);

  void set_cell_val_num(unsigned num);
  void set_filter_list(const FilterList& filter_list);
};

}  // namespace tiledb::rs

#endif
