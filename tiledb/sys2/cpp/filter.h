#ifndef TILEDB_RS_API_FILTER_H
#define TILEDB_RS_API_FILTER_H

#include <memory>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Context;

class Filter {
 public:
  Filter(const Context& ctx, tiledb_filter_t* filter);

  tiledb_filter_type_t filter_type() const;
  void get_option(tiledb_filter_option_t option, void* value);

  std::shared_ptr<tiledb_filter_t> ptr() const;

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_filter_t> filter_;
};

class FilterBuilder {
 public:
  FilterBuilder(std::shared_ptr<const Context> ctx);

  Filter& set_option(tiledb_filter_option_t option, const void* value);

  void set_compression_level(int32_t val);

  void set_bit_width_max_window(uint32_t val);
  void set_positive_delta_max_window(uint32_t val);

  void set_scale_float_bytewidth(uint64_t val);
  void set_scale_float_factor(double val);
  void set_scale_float_offset(double val);

  void set_webp_quality(float val);
  void set_webp_input_format(tiledb_filter_webp_format_t val);
  void set_webp_lossless(uint8_t val);

  void set_compression_reinterpret_datatype(tiledb_datatype_t val);
};

}  // namespace tiledb::rs

#endif
