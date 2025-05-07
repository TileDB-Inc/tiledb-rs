#ifndef TILEDB_RS_API_FILTER_H
#define TILEDB_RS_API_FILTER_H

#include <memory>

#include <tiledb/tiledb.h>

#include "tiledb-sys2/src/datatype.rs.h"
#include "tiledb-sys2/src/filter_type.rs.h"
#include "tiledb-sys2/src/webp_format.rs.h"

namespace tiledb::rs {

class Context;

class Filter {
 public:
  Filter(std::shared_ptr<Context> ctx, tiledb_filter_t* filter);
  Filter(std::shared_ptr<Context> ctx, std::shared_ptr<tiledb_filter_t> filter);

  FilterType get_type() const;

  int32_t get_compression_level() const;
  Datatype get_compression_reinterpret_datatype() const;

  uint32_t get_bit_width_max_window() const;
  uint32_t get_positive_delta_max_window() const;

  uint64_t get_scale_float_bytewidth() const;
  double get_scale_float_factor() const;
  double get_scale_float_offset() const;

  float get_webp_quality() const;
  WebPFormat get_webp_input_format() const;
  bool get_webp_lossless() const;

  std::shared_ptr<tiledb_filter_t> ptr() const;

 private:
  void get_option(tiledb_filter_option_t option, void* value) const;

  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_filter_t> filter_;
};

class FilterBuilder {
 public:
  FilterBuilder(std::shared_ptr<Context> ctx, FilterType filter_type);

  std::shared_ptr<Filter> build() const;

  void set_compression_level(int32_t val) const;
  void set_compression_reinterpret_datatype(Datatype val) const;

  void set_bit_width_max_window(uint32_t val) const;
  void set_positive_delta_max_window(uint32_t val) const;

  void set_scale_float_bytewidth(uint64_t val) const;
  void set_scale_float_factor(double val) const;
  void set_scale_float_offset(double val) const;

  void set_webp_quality(float val) const;
  void set_webp_input_format(WebPFormat val) const;
  void set_webp_lossless(bool val) const;

 private:
  void set_option(tiledb_filter_option_t option, void* value) const;

  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_filter_t> filter_;
};

std::shared_ptr<FilterBuilder> create_filter_builder(
    std::shared_ptr<Context> ctx, FilterType filter_type);

}  // namespace tiledb::rs

#endif
