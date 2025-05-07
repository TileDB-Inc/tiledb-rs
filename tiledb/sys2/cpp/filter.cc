#include "filter.h"
#include "context.h"
#include "datatype.h"
#include "filter_type.h"
#include "webp_format.h"

namespace tiledb::rs {

static void delete_filter(tiledb_filter_t* filter) {
  if (filter != nullptr) {
    tiledb_filter_free(&filter);
  }
}

Filter::Filter(std::shared_ptr<Context> ctx, tiledb_filter_t* filter)
    : ctx_(ctx)
    , filter_(filter, delete_filter) {
}

Filter::Filter(
    std::shared_ptr<Context> ctx, std::shared_ptr<tiledb_filter_t> filter)
    : ctx_(ctx)
    , filter_(filter) {
}

FilterType Filter::get_type() const {
  tiledb_filter_type_t type;
  ctx_->handle_error(
      tiledb_filter_get_type(ctx_->ptr().get(), filter_.get(), &type));
  return to_rs_filter_type(type);
}

int32_t Filter::get_compression_level() const {
  int32_t val;
  get_option(TILEDB_COMPRESSION_LEVEL, &val);
  return val;
}

Datatype Filter::get_compression_reinterpret_datatype() const {
  tiledb_datatype_t type;
  get_option(TILEDB_COMPRESSION_REINTERPRET_DATATYPE, &type);
  return to_rs_datatype(type);
}

uint32_t Filter::get_bit_width_max_window() const {
  uint32_t val;
  get_option(TILEDB_BIT_WIDTH_MAX_WINDOW, &val);
  return val;
}

uint32_t Filter::get_positive_delta_max_window() const {
  uint32_t val;
  get_option(TILEDB_POSITIVE_DELTA_MAX_WINDOW, &val);
  return val;
}

uint64_t Filter::get_scale_float_bytewidth() const {
  uint64_t val;
  get_option(TILEDB_SCALE_FLOAT_BYTEWIDTH, &val);
  return val;
}

double Filter::get_scale_float_factor() const {
  double val;
  get_option(TILEDB_SCALE_FLOAT_FACTOR, &val);
  return val;
}

double Filter::get_scale_float_offset() const {
  double val;
  get_option(TILEDB_SCALE_FLOAT_OFFSET, &val);
  return val;
}

float Filter::get_webp_quality() const {
  float val;
  get_option(TILEDB_WEBP_QUALITY, &val);
  return val;
}

WebPFormat Filter::get_webp_input_format() const {
  tiledb_filter_webp_format_t val;
  get_option(TILEDB_WEBP_INPUT_FORMAT, &val);
  return to_rs_webp_format(val);
}

bool Filter::get_webp_lossless() const {
  uint8_t val;
  get_option(TILEDB_WEBP_LOSSLESS, &val);
  return val != 0;
}

std::shared_ptr<tiledb_filter_t> Filter::ptr() const {
  return filter_;
}

void Filter::get_option(tiledb_filter_option_t option, void* value) const {
  ctx_->handle_error(tiledb_filter_get_option(
      ctx_->ptr().get(), filter_.get(), option, value));
}

FilterBuilder::FilterBuilder(
    std::shared_ptr<Context> ctx, FilterType filter_type)
    : ctx_(ctx) {
  auto c_ftype = to_cpp_filter_type(filter_type);
  tiledb_filter_t* filter;
  ctx_->handle_error(tiledb_filter_alloc(ctx_->ptr().get(), c_ftype, &filter));
  filter_ = std::shared_ptr<tiledb_filter_t>(filter, delete_filter);
}

std::shared_ptr<Filter> FilterBuilder::build() const {
  return std::make_shared<Filter>(ctx_, filter_);
}

void FilterBuilder::set_compression_level(int32_t val) const {
  set_option(TILEDB_COMPRESSION_LEVEL, &val);
}

void FilterBuilder::set_compression_reinterpret_datatype(Datatype val) const {
  auto c_val = to_cpp_datatype(val);
  set_option(TILEDB_COMPRESSION_REINTERPRET_DATATYPE, &c_val);
}

void FilterBuilder::set_bit_width_max_window(uint32_t val) const {
  set_option(TILEDB_BIT_WIDTH_MAX_WINDOW, &val);
}
void FilterBuilder::set_positive_delta_max_window(uint32_t val) const {
  set_option(TILEDB_POSITIVE_DELTA_MAX_WINDOW, &val);
}

void FilterBuilder::set_scale_float_bytewidth(uint64_t val) const {
  set_option(TILEDB_SCALE_FLOAT_BYTEWIDTH, &val);
}

void FilterBuilder::set_scale_float_factor(double val) const {
  set_option(TILEDB_SCALE_FLOAT_FACTOR, &val);
}

void FilterBuilder::set_scale_float_offset(double val) const {
  set_option(TILEDB_SCALE_FLOAT_OFFSET, &val);
}

void FilterBuilder::set_webp_quality(float val) const {
  set_option(TILEDB_WEBP_QUALITY, &val);
}

void FilterBuilder::set_webp_input_format(WebPFormat val) const {
  auto c_val = to_cpp_webp_format(val);
  set_option(TILEDB_WEBP_INPUT_FORMAT, &c_val);
}

void FilterBuilder::set_webp_lossless(bool val) const {
  auto c_val = val ? 1 : 0;
  set_option(TILEDB_WEBP_LOSSLESS, &c_val);
}

void FilterBuilder::set_option(tiledb_filter_option_t opt, void* val) const {
  ctx_->handle_error(
      tiledb_filter_set_option(ctx_->ptr().get(), filter_.get(), opt, &val));
}

std::shared_ptr<FilterBuilder> create_filter_builder(
    std::shared_ptr<Context> ctx, FilterType filter_type) {
  return std::make_shared<FilterBuilder>(ctx, filter_type);
}
}  // namespace tiledb::rs
