#ifndef TILEDB_RS_API_DIMENSION_H
#define TILEDB_RS_API_DIMENSION_H

#include <tiledb/tiledb.h>

#include "rust/cxx.h"
#include "tiledb-sys2/src/datatype.rs.h"

namespace tiledb::rs {

class Context;
class FilterList;

class Dimension {
 public:
  Dimension(const Context& ctx, tiledb_dimension_t* dim);

  rust::String name() const;
  Datatype type() const;
  uint32_t cell_val_num() const;
  std::shared_ptr<FilterList> filter_list() const;

  template <typename T>
  std::pair<T, T> domain() const;

  template <typename T>
  T tile_extent() const;

  std::shared_ptr<tiledb_dimension_t> ptr() const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_dimension_t> dim_;
};

class DimensionBuilder {
 public:
  DimensionBuilder(std::shared_ptr<Context> ctx, rust::Str name, Datatype type);

  std::shared_ptr<Dimension> build() const;

  void set_domain_i8(rust::Slice<int8_t> lower, rust::Slice<int8_t> upper);
  void set_domain_i16(rust::Slice<int16_t> lower, rust::Slice<int16_t> upper);
  void set_domain_i32(rust::Slice<int32_t> lower, rust::Slice<int32_t> upper);
  void set_domain_i64(rust::Slice<int64_t> lower, rust::Slice<int64_t> upper);
  void set_domain_u8(rust::Slice<uint8_t> lower, rust::Slice<uint8_t> upper);
  void set_domain_u16(rust::Slice<uint16_t> lower, rust::Slice<uint16_t> upper);
  void set_domain_u32(rust::Slice<uint32_t> lower, rust::Slice<uint32_t> upper);
  void set_domain_u64(rust::Slice<uint64_t> lower, rust::Slice<uint64_t> upper);
  void set_domain_f32(rust::Slice<float> lower, rust::Slice<float> upper);
  void set_domain_f64(rust::Slice<double> lower, rust::Slice<double> upper);

  template <typename T>
  void set_domain(rust::Slice<T> lower, rust::Slice<T> upper);

  void set_extent_i8(int8_t val);
  void set_extent_i16(int16_t val);
  void set_extent_i32(int32_t val);
  void set_extent_i64(int64_t val);
  void set_extent_u8(uint8_t val);
  void set_extent_u16(uint16_t val);
  void set_extent_u32(uint32_t val);
  void set_extent_u64(uint64_t val);
  void set_extent_f32(float val);
  void set_extent_f64(double val);

  template <typename T>
  void set_extent(T val);

  void set_cell_val_num(uint32_t num) const;
  void set_filter_list(const FilterList& filter_list) const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_dimension_t> dim_;
};

}  // namespace tiledb::rs

#endif
