#include <memory>
#include <string>

#include <tiledb/tiledb_experimental.h>

#include "attribute.h"
#include "context.h"
#include "datatype.h"
#include "filter_list.h"

namespace tiledb::rs {

static void delete_attribute(tiledb_attribute_t* attr) {
  if (attr != nullptr) {
    tiledb_attribute_free(&attr);
  }
}

Attribute::Attribute(std::shared_ptr<Context> ctx, tiledb_attribute_t* attr)
    : ctx_(ctx)
    , attr_(attr, delete_attribute) {
}

Attribute::Attribute(
    std::shared_ptr<Context> ctx, std::shared_ptr<tiledb_attribute_t> attr)
    : ctx_(ctx)
    , attr_(attr) {
}

rust::String Attribute::name() const {
  const char* name;
  ctx_->handle_error(
      tiledb_attribute_get_name(ctx_->ptr().get(), attr_.get(), &name));
  return name;
}

Datatype Attribute::datatype() const {
  tiledb_datatype_t type;
  ctx_->handle_error(
      tiledb_attribute_get_type(ctx_->ptr().get(), attr_.get(), &type));
  return to_rs_datatype(type);
}

uint64_t Attribute::cell_size() const {
  uint64_t val;
  ctx_->handle_error(
      tiledb_attribute_get_cell_size(ctx_->ptr().get(), attr_.get(), &val));
  return val;
}

uint32_t Attribute::cell_val_num() const {
  uint32_t val;
  ctx_->handle_error(
      tiledb_attribute_get_cell_val_num(ctx_->ptr().get(), attr_.get(), &val));
  return val;
}

bool Attribute::nullable() const {
  uint8_t val;
  ctx_->handle_error(
      tiledb_attribute_get_nullable(ctx_->ptr().get(), attr_.get(), &val));
  return val != 0;
}

bool Attribute::enumeration_name(rust::String& name) const {
  tiledb_string_t* enmr_name;
  ctx_->handle_error(tiledb_attribute_get_enumeration_name(
      ctx_->ptr().get(), attr_.get(), &enmr_name));

  if (enmr_name == nullptr) {
    return false;
  }

  // Convert string handle to a std::string
  const char* name_ptr;
  size_t name_len;
  ctx_->handle_error(tiledb_string_view(enmr_name, &name_ptr, &name_len));
  name = rust::String(name_ptr, name_len);

  // Release the string handle
  ctx_->handle_error(tiledb_string_free(&enmr_name));

  return true;
}

std::shared_ptr<FilterList> Attribute::filter_list() const {
  tiledb_filter_list_t* filter_list;
  ctx_->handle_error(tiledb_attribute_get_filter_list(
      ctx_->ptr().get(), attr_.get(), &filter_list));
  return std::make_shared<FilterList>(ctx_, filter_list);
}

void Attribute::fill_value(Buffer& buf) const {
  tiledb_datatype_t dtype;
  ctx_->handle_error(
      tiledb_attribute_get_type(ctx_->ptr().get(), attr_.get(), &dtype));
  auto dt_size = tiledb_datatype_size(dtype);

  const void* data = nullptr;
  uint64_t size = 0;
  ctx_->handle_error(tiledb_attribute_get_fill_value(
      ctx_->ptr().get(), attr_.get(), &data, &size));

  buf.resize(size / dt_size);
  std::memcpy(buf.as_mut_ptr(), data, size);
}

void Attribute::fill_value_nullable(Buffer& buf, uint8_t& validity) const {
  tiledb_datatype_t dtype;
  ctx_->handle_error(
      tiledb_attribute_get_type(ctx_->ptr().get(), attr_.get(), &dtype));
  auto dt_size = tiledb_datatype_size(dtype);

  const void* data = nullptr;
  uint64_t size = 0;
  ctx_->handle_error(tiledb_attribute_get_fill_value_nullable(
      ctx_->ptr().get(), attr_.get(), &data, &size, &validity));

  buf.resize(size / dt_size);
  std::memcpy(buf.as_mut_ptr(), data, size);
}

AttributeBuilder::AttributeBuilder(
    std::shared_ptr<Context> ctx, std::string name, tiledb_datatype_t dtype)
    : ctx_(ctx) {
  tiledb_attribute_t* attr;
  ctx->handle_error(
      tiledb_attribute_alloc(ctx_->ptr().get(), name.c_str(), dtype, &attr));
  attr_ = std::shared_ptr<tiledb_attribute_t>(attr, delete_attribute);
}

std::shared_ptr<Attribute> AttributeBuilder::build() const {
  return std::make_shared<Attribute>(ctx_, attr_);
}

void AttributeBuilder::set_nullable(bool nullable) const {
  ctx_->handle_error(tiledb_attribute_set_nullable(
      ctx_->ptr().get(), attr_.get(), static_cast<uint8_t>(nullable)));
}

void AttributeBuilder::set_cell_val_num(unsigned num) const {
  ctx_->handle_error(
      tiledb_attribute_set_cell_val_num(ctx_->ptr().get(), attr_.get(), num));
}

void AttributeBuilder::set_enumeration_name(
    const rust::Str enumeration_name) const {
  auto c_name = static_cast<std::string>(enumeration_name);
  ctx_->handle_error(tiledb_attribute_set_enumeration_name(
      ctx_->ptr().get(), attr_.get(), c_name.c_str()));
}

void AttributeBuilder::set_filter_list(
    std::shared_ptr<FilterList> filter_list) const {
  ctx_->handle_error(tiledb_attribute_set_filter_list(
      ctx_->ptr().get(), attr_.get(), filter_list->ptr().get()));
}

void AttributeBuilder::set_fill_value(Buffer& value) const {
  ctx_->handle_error(tiledb_attribute_set_fill_value(
      ctx_->ptr().get(), attr_.get(), value.as_ptr(), value.len()));
}

void AttributeBuilder::set_fill_value_nullable(
    Buffer& value, uint8_t validity) const {
  ctx_->handle_error(tiledb_attribute_set_fill_value_nullable(
      ctx_->ptr().get(), attr_.get(), value.as_ptr(), value.len(), validity));
}

std::shared_ptr<AttributeBuilder> create_attribute_builder(
    std::shared_ptr<Context> ctx, rust::Str name, Datatype dtype) {
  auto c_name = static_cast<std::string>(name);
  auto c_dtype = to_cpp_datatype(dtype);
  return std::make_shared<AttributeBuilder>(ctx, c_name, c_dtype);
}

}  // namespace tiledb::rs
