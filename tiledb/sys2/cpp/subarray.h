#ifndef TILEDB_RS_API_SUBARRAY_H
#define TILEDB_RS_API_SUBARRAY_H

#include <memory>
#include <string>
#include <vector>

#include <tiledb/tiledb.h>

#include "rust/cxx.h"

namespace tiledb::rs {

class Array;
class Config;
class Context;

class Subarray {
 public:
  Subarray(std::shared_ptr<Context> ctx, std::shared_ptr<Array> array);

  uint64_t num_ranges_from_index(uint32_t idx);
  uint64_t num_ranges_from_name(rust::Str name);

  void get_range_from_index(
      uint32_t dim_idx,
      uint64_t range_idx,
      const void** start,
      const void** end,
      const void** stride);
  void get_range_from_name(const char* name, ...);

  void get_range_var_size(
      uint32_t dim_idx, uint64_t range_idx, uint64_t& start, uint64_t& end);
  void get_range_var_size_from_name(const char* name, ...);

  void get_range_var(uint32_t idx, uint64_t range_idx, void* start, void* end);
  void get_range_var_from_name(const char* name, ...);

  void add_point_ranges(uint32_t dim_idx, const void* start, uint64_t count);
  void add_point_ranges_var(
      uint32_t dim_idx,
      const void* start,
      uint64_t start_size,
      const uint64_t* start_offsets,
      uint64_t start_offsets_size);

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<Array> array_;
  std::shared_ptr<tiledb_subarray_t> subarray_;
};

class SubarrayBuilder {
 public:
  void set_coalesce_ranges(bool coalesce_ranges) const;
  void set_config(std::shared_ptr<Config> cfg) const;

  // The docs say this is only for dense writes and takes a list of [low, high]
  // pairs that must match the dimensions types of the domain.
  //
  // Maybe ignore
  void set_subarray(void* subarray) const;

  // N.B., the stride is optional.

  // This needs to be morphed to probably take a buffer of length 3
  // to pass the values. Which makes me suddenly wonder about adding a stack
  // based version of the buffer class.
  void add_range(
      uint32_t dim_idx, const void* start, const void* end, const void* stride);

  // Same comment as add_range
  void add_range_by_name(const char* dim_name, ...);
  void add_range_var(uint32_t dim_idx, ...);
  void add_range_var_by_name(const char* dim_name, ...);
};

}  // namespace tiledb::rs

#endif
