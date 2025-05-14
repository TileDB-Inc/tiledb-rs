#include <memory>
#include <string>

#include <tiledb/tiledb_experimental.h>

#include "context.h"
#include "datatype.h"
#include "dimension.h"
#include "filter_list.h"

namespace tiledb::rs {

static void delete_dimension(tiledb_dimension_t* dim) {
  if (dim != nullptr) {
    tiledb_dimension_free(&dim);
  }
}

Dimension::Dimension(std::shared_ptr<Context> ctx, tiledb_dimension_t* dim)
    : ctx_(ctx)
    , dim_(dim, delete_dimension) {
}

Dimension::Dimension(
    std::shared_ptr<Context> ctx, std::shared_ptr<tiledb_dimension_t> dim)
    : ctx_(ctx)
    , dim_(dim) {
}

rust::String Dimension::name() const {
  const char* name;
  ctx_->handle_error(
      tiledb_dimension_get_name(ctx_->ptr().get(), dim_.get(), &name));
  return name;
}

Datatype Dimension::datatype() const {
  tiledb_datatype_t dt;
  ctx_->handle_error(
      tiledb_dimension_get_type(ctx_->ptr().get(), dim_.get(), &dt));
  return to_rs_datatype(dt);
}

bool Dimension::domain(Buffer& value) const {
  auto dtype = to_cpp_datatype(datatype());
  if (dtype == TILEDB_STRING_ASCII || dtype == TILEDB_STRING_UTF8) {
    return false;
  }

  auto cvn = cell_val_num();
  if (cvn == 0 || cvn == std::numeric_limits<uint32_t>::max()) {
    return false;
  }

  const void* c_dom = nullptr;
  ctx_->handle_error(
      tiledb_dimension_get_domain(ctx_->ptr().get(), dim_.get(), &c_dom));

  value.resize(cvn * 2);
  std::memcpy(value.as_mut_ptr(), c_dom, value.len());

  return true;
}

bool Dimension::tile_extent(Buffer& value) const {
  auto dtype = to_cpp_datatype(datatype());
  if (dtype == TILEDB_STRING_ASCII || dtype == TILEDB_STRING_UTF8 ||
      dtype == TILEDB_FLOAT32 || dtype == TILEDB_FLOAT64) {
    return false;
  }

  auto cvn = cell_val_num();
  if (cvn == 0 || cvn == std::numeric_limits<uint32_t>::max()) {
    return false;
  }

  const void* c_ext = nullptr;
  ctx_->handle_error(
      tiledb_dimension_get_tile_extent(ctx_->ptr().get(), dim_.get(), &c_ext));

  auto size = tiledb_datatype_size(dtype);
  value.resize(cvn);
  std::memcpy(value.as_mut_ptr(), c_ext, size * cvn);

  return true;
}

uint32_t Dimension::cell_val_num() const {
  uint32_t cvn = 0;
  ctx_->handle_error(
      tiledb_dimension_get_cell_val_num(ctx_->ptr().get(), dim_.get(), &cvn));
  return cvn;
}

std::shared_ptr<FilterList> Dimension::filter_list() const {
  tiledb_filter_list_t* list = nullptr;
  ctx_->handle_error(
      tiledb_dimension_get_filter_list(ctx_->ptr().get(), dim_.get(), &list));
  return std::make_shared<FilterList>(ctx_, list);
}

std::shared_ptr<tiledb_dimension_t> Dimension::ptr() const {
  return dim_;
}

DimensionBuilder::DimensionBuilder(
    std::shared_ptr<Context> ctx,
    rust::Str name,
    Datatype dtype,
    Buffer& domain,
    Buffer& extent)
    : ctx_(ctx) {
  auto c_name = static_cast<std::string>(name);
  auto c_dtype = to_cpp_datatype(dtype);
  void* c_domain = nullptr;
  void* c_extent = nullptr;

  if (domain.len() > 0) {
    c_domain = static_cast<void*>(domain.as_mut_ptr());
  }

  if (extent.len() > 0) {
    c_extent = static_cast<void*>(extent.as_mut_ptr());
  }

  tiledb_dimension_t* dim = nullptr;
  ctx_->handle_error(tiledb_dimension_alloc(
      ctx_->ptr().get(), c_name.c_str(), c_dtype, c_domain, c_extent, &dim));

  dim_ = std::shared_ptr<tiledb_dimension_t>(dim, delete_dimension);
}

std::shared_ptr<Dimension> DimensionBuilder::build() const {
  return std::make_shared<Dimension>(ctx_, dim_);
}

void DimensionBuilder::set_cell_val_num(uint32_t cell_val_num) const {
  ctx_->handle_error(tiledb_dimension_set_cell_val_num(
      ctx_->ptr().get(), dim_.get(), cell_val_num));
}

void DimensionBuilder::set_filter_list(
    std::shared_ptr<FilterList> filters) const {
  ctx_->handle_error(tiledb_dimension_set_filter_list(
      ctx_->ptr().get(), dim_.get(), filters->ptr().get()));
}

std::shared_ptr<DimensionBuilder> create_dimension_builder(
    std::shared_ptr<Context> ctx,
    rust::Str name,
    Datatype dtype,
    Buffer& domain,
    Buffer& extent) {
  return std::make_shared<DimensionBuilder>(ctx, name, dtype, domain, extent);
}

}  // namespace tiledb::rs
