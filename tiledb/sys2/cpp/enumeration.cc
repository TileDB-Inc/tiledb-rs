#include <memory>
#include <string>

#include <tiledb/tiledb_experimental.h>

#include "context.h"
#include "datatype.h"
#include "enumeration.h"
#include "string.h"

namespace tiledb::rs {

static void delete_enumeration(tiledb_enumeration_t* enmr) {
  if (enmr != nullptr) {
    tiledb_enumeration_free(&enmr);
  }
}

Enumeration::Enumeration(
    std::shared_ptr<Context> ctx, tiledb_enumeration_t* enmr)
    : ctx_(ctx)
    , enmr_(enmr, delete_enumeration) {
}

rust::String Enumeration::name() const {
  tiledb_string_t* name;
  ctx_->handle_error(
      tiledb_enumeration_get_name(ctx_->ptr().get(), enmr_.get(), &name));

  String tdb(name);
  std::string c_name(tdb.view());
  return c_name;
}

Datatype Enumeration::datatype() const {
  tiledb_datatype_t dtype;
  ctx_->handle_error(
      tiledb_enumeration_get_type(ctx_->ptr().get(), enmr_.get(), &dtype));
  return to_rs_datatype(dtype);
}

uint32_t Enumeration::cell_val_num() const {
  uint32_t cvn;
  ctx_->handle_error(tiledb_enumeration_get_cell_val_num(
      ctx_->ptr().get(), enmr_.get(), &cvn));
  return cvn;
}

bool Enumeration::ordered() const {
  int ordered;
  ctx_->handle_error(
      tiledb_enumeration_get_ordered(ctx_->ptr().get(), enmr_.get(), &ordered));
  return ordered != 0;
}

void Enumeration::get_data(Buffer& buf) const {
  const void* data = nullptr;
  uint64_t size = 0;
  ctx_->handle_error(tiledb_enumeration_get_data(
      ctx_->ptr().get(), enmr_.get(), &data, &size));

  buf.resize_bytes(size);
  std::memcpy(buf.as_mut_ptr(), data, size);
}

void Enumeration::get_offsets(Buffer& buf) const {
  const void* data = nullptr;
  uint64_t size = 0;
  ctx_->handle_error(tiledb_enumeration_get_offsets(
      ctx_->ptr().get(), enmr_.get(), &data, &size));

  buf.resize_bytes(size);
  std::memcpy(buf.as_mut_ptr(), data, size);
}

bool Enumeration::get_index(Buffer& buf, uint64_t& index) const {
  auto data = buf.as_mut_ptr();
  auto size = buf.len();

  int exists;
  ctx_->handle_error(tiledb_enumeration_get_value_index(
      ctx_->ptr().get(), enmr_.get(), data, size, &exists, &index));
  return exists != 0;
}

std::shared_ptr<Enumeration> Enumeration::extend(
    Buffer& data, Buffer& offsets) const {
  auto data_ptr = data.as_mut_ptr();
  auto data_size = data.len();
  auto off_ptr = offsets.as_mut_ptr();
  auto off_size = offsets.len();

  tiledb_enumeration_t* enmr;
  ctx_->handle_error(tiledb_enumeration_extend(
      ctx_->ptr().get(),
      enmr_.get(),
      data_ptr,
      data_size,
      off_ptr,
      off_size,
      &enmr));

  return std::make_shared<Enumeration>(ctx_, enmr);
}

std::shared_ptr<tiledb_enumeration_t> Enumeration::ptr() const {
  return enmr_;
}

std::shared_ptr<Enumeration> create_enumeration(
    std::shared_ptr<Context> ctx,
    rust::Str name,
    Datatype type,
    uint32_t cell_val_num,
    bool ordered,
    Buffer& data,
    Buffer& offsets) {
  auto c_name = static_cast<std::string>(name);
  auto c_type = to_cpp_datatype(type);
  int c_ordered = ordered ? 1 : 0;

  auto data_ptr = data.as_mut_ptr();
  auto data_size = data.len();
  auto off_ptr = offsets.as_mut_ptr();
  auto off_size = offsets.len();

  tiledb_enumeration_t* enmr;
  ctx->handle_error(tiledb_enumeration_alloc(
      ctx->ptr().get(),
      c_name.c_str(),
      c_type,
      cell_val_num,
      c_ordered,
      data_ptr,
      data_size,
      off_ptr,
      off_size,
      &enmr));

  return std::make_shared<Enumeration>(ctx, enmr);
}

}  // namespace tiledb::rs
