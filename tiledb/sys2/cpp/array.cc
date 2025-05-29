#include <memory>
#include <sstream>
#include <string>

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

#include "tiledb-sys2/src/utils.rs.h"

#include "array.h"
#include "context.h"
#include "datatype.h"
#include "dimension.h"
#include "domain.h"
#include "enumeration.h"
#include "exception.h"
#include "mode.h"
#include "schema.h"

namespace tiledb::rs {

static void delete_array(tiledb_array_t* array) {
  if (array != nullptr) {
    tiledb_array_free(&array);
  }
}

Array::Array(std::shared_ptr<Context> ctx, tiledb_array_t* array)
    : ctx_(ctx)
    , array_(array, delete_array) {
}

Array::Array(std::shared_ptr<Context> ctx, rust::Str uri)
    : ctx_(ctx) {
  auto c_uri = static_cast<std::string>(uri);
  tiledb_array_t* array = nullptr;
  ctx_->handle_error(
      tiledb_array_alloc(ctx_->ptr().get(), c_uri.c_str(), &array));
  array_ = std::shared_ptr<tiledb_array_t>(array, delete_array);
}

rust::String Array::uri() const {
  const char* uri = nullptr;
  ctx_->handle_error(
      tiledb_array_get_uri(ctx_->ptr().get(), array_.get(), &uri));
  return uri;
}

void Array::set_config(std::shared_ptr<Config> cfg) const {
  ctx_->handle_error(tiledb_array_set_config(
      ctx_->ptr().get(), array_.get(), cfg->ptr().get()));
}

void Array::set_open_timestamp_start(uint64_t ts) const {
  ctx_->handle_error(tiledb_array_set_open_timestamp_start(
      ctx_->ptr().get(), array_.get(), ts));
}

void Array::set_open_timestamp_end(uint64_t ts) const {
  ctx_->handle_error(
      tiledb_array_set_open_timestamp_end(ctx_->ptr().get(), array_.get(), ts));
}

void Array::open(Mode mode) const {
  auto c_mode = to_cpp_mode(mode);
  ctx_->handle_error(
      tiledb_array_open(ctx_->ptr().get(), array_.get(), c_mode));
}

void Array::reopen() const {
  ctx_->handle_error(tiledb_array_reopen(ctx_->ptr().get(), array_.get()));
}

void Array::close() const {
  ctx_->handle_error(tiledb_array_close(ctx_->ptr().get(), array_.get()));
}

bool Array::is_open() const {
  int is_open;
  ctx_->handle_error(
      tiledb_array_is_open(ctx_->ptr().get(), array_.get(), &is_open));
  return is_open != 0;
}

Mode Array::mode() const {
  tiledb_query_type_t mode;
  ctx_->handle_error(
      tiledb_array_get_query_type(ctx_->ptr().get(), array_.get(), &mode));
  return to_rs_mode(mode);
}

std::shared_ptr<Config> Array::config() const {
  tiledb_config_t* cfg = nullptr;
  ctx_->handle_error(
      tiledb_array_get_config(ctx_->ptr().get(), array_.get(), &cfg));
  return std::make_shared<Config>(cfg);
}

std::shared_ptr<Schema> Array::schema() const {
  tiledb_array_schema_t* schema = nullptr;
  ctx_->handle_error(
      tiledb_array_get_schema(ctx_->ptr().get(), array_.get(), &schema));
  return std::make_shared<Schema>(ctx_, schema);
}

uint64_t Array::open_timestamp_start() const {
  uint64_t ts;
  ctx_->handle_error(tiledb_array_get_open_timestamp_start(
      ctx_->ptr().get(), array_.get(), &ts));
  return ts;
}

uint64_t Array::open_timestamp_end() const {
  uint64_t ts;
  ctx_->handle_error(tiledb_array_get_open_timestamp_end(
      ctx_->ptr().get(), array_.get(), &ts));
  return ts;
}

std::shared_ptr<Enumeration> Array::get_enumeration(rust::Str enmr_name) const {
  auto c_name = static_cast<std::string>(enmr_name);
  tiledb_enumeration_t* enmr = nullptr;
  ctx_->handle_error(tiledb_array_get_enumeration(
      ctx_->ptr().get(), array_.get(), c_name.c_str(), &enmr));
  return std::make_shared<Enumeration>(ctx_, enmr);
}

void Array::load_all_enumerations() const {
  ctx_->handle_error(
      tiledb_array_load_all_enumerations(ctx_->ptr().get(), array_.get()));
}

void Array::load_enumerations_all_schemas() const {
  ctx_->handle_error(tiledb_array_load_enumerations_all_schemas(
      ctx_->ptr().get(), array_.get()));
}

bool Array::non_empty_domain_from_index(uint32_t index, Buffer& buffer) const {
  auto dim = schema()->domain()->dimension_from_index(index);
  if (!buffer.cpp_is_compatible_type(static_cast<uint8_t>(dim->datatype()))) {
    throw TileDBError(
        "Non-empty domain buffer was allocated with the wrong datatype.");
  }

  buffer.resize(2);
  int empty;

  ctx_->handle_error(tiledb_array_get_non_empty_domain_from_index(
      ctx_->ptr().get(), array_.get(), index, buffer.as_mut_ptr(), &empty));

  return empty != 0;
}

bool Array::non_empty_domain_from_name(rust::Str name, Buffer& buffer) const {
  auto dim = schema()->domain()->dimension_from_name(name);
  if (!buffer.cpp_is_compatible_type(static_cast<uint8_t>(dim->datatype()))) {
    throw TileDBError(
        "Non-empty domain buffer was allocated with the wronge datatype.");
  }

  auto c_name = static_cast<std::string>(name);
  buffer.resize(2);
  int empty;

  ctx_->handle_error(tiledb_array_get_non_empty_domain_from_name(
      ctx_->ptr().get(),
      array_.get(),
      c_name.c_str(),
      buffer.as_mut_ptr(),
      &empty));

  return empty != 0;
}

bool Array::non_empty_domain_var_from_index(
    uint32_t index, Buffer& lower, Buffer& upper) const {
  auto dim = schema()->domain()->dimension_from_index(index);
  if (!lower.cpp_is_compatible_type(static_cast<uint8_t>(dim->datatype()))) {
    throw TileDBError(
        "Non-empty domain lower buffer was allocated with the wronge "
        "datatype.");
  }

  if (!upper.cpp_is_compatible_type(static_cast<uint8_t>(dim->datatype()))) {
    throw TileDBError(
        "Non-empty domain upper buffer was allocated with the wronge "
        "datatype.");
  }

  uint64_t lower_size;
  uint64_t upper_size;
  int empty;

  ctx_->handle_error(tiledb_array_get_non_empty_domain_var_size_from_index(
      ctx_->ptr().get(),
      array_.get(),
      index,
      &lower_size,
      &upper_size,
      &empty));

  if (empty != 0) {
    return false;
  }

  lower.resize_bytes(lower_size);
  upper.resize_bytes(upper_size);

  ctx_->handle_error(tiledb_array_get_non_empty_domain_var_from_index(
      ctx_->ptr().get(),
      array_.get(),
      index,
      lower.as_mut_ptr(),
      upper.as_mut_ptr(),
      &empty));

  return empty != 0;
}

bool Array::non_empty_domain_var_from_name(
    rust::Str name, Buffer& lower, Buffer& upper) const {
  auto dim = schema()->domain()->dimension_from_name(name);
  if (!lower.cpp_is_compatible_type(static_cast<uint8_t>(dim->datatype()))) {
    throw TileDBError(
        "Non-empty domain lower buffer was allocated with the wronge "
        "datatype.");
  }

  if (!upper.cpp_is_compatible_type(static_cast<uint8_t>(dim->datatype()))) {
    throw TileDBError(
        "Non-empty domain upper buffer was allocated with the wronge "
        "datatype.");
  }

  auto c_name = static_cast<std::string>(name);
  uint64_t lower_size;
  uint64_t upper_size;
  int empty;

  ctx_->handle_error(tiledb_array_get_non_empty_domain_var_size_from_name(
      ctx_->ptr().get(),
      array_.get(),
      c_name.c_str(),
      &lower_size,
      &upper_size,
      &empty));

  if (empty != 0) {
    return false;
  }

  lower.resize_bytes(lower_size);
  upper.resize_bytes(upper_size);

  ctx_->handle_error(tiledb_array_get_non_empty_domain_var_from_name(
      ctx_->ptr().get(),
      array_.get(),
      c_name.c_str(),
      lower.as_mut_ptr(),
      upper.as_mut_ptr(),
      &empty));

  return empty != 0;
}

void Array::put_metadata(
    rust::Str key, Datatype dtype, uint32_t num, Buffer& values) const {
  auto c_key = static_cast<std::string>(key);
  auto c_dtype = to_cpp_datatype(dtype);

  ctx_->handle_error(tiledb_array_put_metadata(
      ctx_->ptr().get(),
      array_.get(),
      c_key.c_str(),
      c_dtype,
      num,
      values.as_mut_ptr()));
}

void Array::get_metadata(rust::Str key, Datatype& dtype, Buffer& values) const {
  auto c_key = static_cast<std::string>(key);
  tiledb_datatype_t c_dtype;
  uint32_t c_num;
  const void* c_data = nullptr;

  ctx_->handle_error(tiledb_array_get_metadata(
      ctx_->ptr().get(),
      array_.get(),
      c_key.c_str(),
      &c_dtype,
      &c_num,
      &c_data));

  if (c_data == nullptr) {
    std::stringstream ss;
    ss << "Metadata key '" << c_key << "' was not found.";
    throw TileDBError(ss.str());
  }

  dtype = to_rs_datatype(c_dtype);
  values.cpp_init(static_cast<uint8_t>(dtype));
  values.resize(c_num);
  std::memcpy(values.as_mut_ptr(), c_data, values.len());
}

void Array::delete_metadata(rust::Str key) const {
  auto c_key = static_cast<std::string>(key);
  ctx_->handle_error(tiledb_array_delete_metadata(
      ctx_->ptr().get(), array_.get(), c_key.c_str()));
}

bool Array::has_metadata(rust::Str key, Datatype& dtype) const {
  auto c_key = static_cast<std::string>(key);
  tiledb_datatype_t c_dtype;
  int32_t exists;
  ctx_->handle_error(tiledb_array_has_metadata_key(
      ctx_->ptr().get(), array_.get(), c_key.c_str(), &c_dtype, &exists));

  dtype = to_rs_datatype(c_dtype);
  return exists != 0;
}

uint64_t Array::num_metadata() const {
  uint64_t num;
  ctx_->handle_error(
      tiledb_array_get_metadata_num(ctx_->ptr().get(), array_.get(), &num));
  return num;
}

void Array::get_metadata_from_index(
    uint64_t index,
    rust::Vec<uint8_t>& key,
    Datatype& dtype,
    Buffer& values) const {
  const char* c_key = nullptr;
  uint32_t c_key_len;
  tiledb_datatype_t c_dtype;
  uint32_t c_num;
  const void* c_values = nullptr;

  ctx_->handle_error(tiledb_array_get_metadata_from_index(
      ctx_->ptr().get(),
      array_.get(),
      index,
      &c_key,
      &c_key_len,
      &c_dtype,
      &c_num,
      &c_values));

  vec_u8_resize(key, c_key_len, 0);
  std::memcpy(key.data(), c_key, c_key_len);

  dtype = to_rs_datatype(c_dtype);

  values.cpp_init(static_cast<uint8_t>(dtype));
  values.resize(c_num);
  std::memcpy(values.as_mut_ptr(), c_values, values.len());
}

std::shared_ptr<tiledb_array_t> Array::ptr() const {
  return array_;
}

std::shared_ptr<Array> create_array(
    std::shared_ptr<Context> ctx, rust::Str uri) {
  return std::make_shared<Array>(ctx, uri);
}

ArrayContext::ArrayContext(std::shared_ptr<Context> ctx, rust::Str uri)
    : ctx_(ctx)
    , uri_(uri) {
}

void ArrayContext::create(std::shared_ptr<Schema> schema) const {
  ctx_->handle_error(
      tiledb_array_schema_check(ctx_->ptr().get(), schema->ptr().get()));
  ctx_->handle_error(tiledb_array_create(
      ctx_->ptr().get(), uri_.c_str(), schema->ptr().get()));
}

void ArrayContext::destroy() const {
  ctx_->handle_error(tiledb_array_delete(ctx_->ptr().get(), uri_.c_str()));
}

void ArrayContext::consolidate() const {
  ctx_->handle_error(
      tiledb_array_consolidate(ctx_->ptr().get(), uri_.c_str(), nullptr));
}

void ArrayContext::consolidate_with_config(std::shared_ptr<Config> cfg) const {
  ctx_->handle_error(tiledb_array_consolidate(
      ctx_->ptr().get(), uri_.c_str(), cfg->ptr().get()));
}

void ArrayContext::consolidate_list(rust::Slice<const rust::Str> uris) const {
  std::vector<std::string> c_uris(uris.size());
  std::vector<const char*> c_uri_ptrs(uris.size());

  rust_to_cpp(uris, c_uris, c_uri_ptrs);

  ctx_->handle_error(tiledb_array_consolidate_fragments(
      ctx_->ptr().get(),
      uri_.c_str(),
      c_uri_ptrs.data(),
      c_uri_ptrs.size(),
      nullptr));
}

void ArrayContext::consolidate_list_with_config(
    rust::Slice<const rust::Str> uris, std::shared_ptr<Config> cfg) const {
  std::vector<std::string> c_uris(uris.size());
  std::vector<const char*> c_uri_ptrs(uris.size());

  rust_to_cpp(uris, c_uris, c_uri_ptrs);

  ctx_->handle_error(tiledb_array_consolidate_fragments(
      ctx_->ptr().get(),
      uri_.c_str(),
      c_uri_ptrs.data(),
      c_uri_ptrs.size(),
      cfg->ptr().get()));
}

void ArrayContext::consolidate_metadata() const {
  auto cfg = Config();
  cfg.set("sm.consolidation.mode", "array_meta");
  ctx_->handle_error(tiledb_array_consolidate(
      ctx_->ptr().get(), uri_.c_str(), cfg.ptr().get()));
}

void ArrayContext::consolidate_metadata_with_config(
    std::shared_ptr<Config> cfg) const {
  cfg->set("sm.consolidation.mode", "array_meta");
  ctx_->handle_error(tiledb_array_consolidate(
      ctx_->ptr().get(), uri_.c_str(), cfg->ptr().get()));
}

void ArrayContext::delete_fragments(
    uint64_t timestamp_start, uint64_t timestamp_end) const {
  ctx_->handle_error(tiledb_array_delete_fragments_v2(
      ctx_->ptr().get(), uri_.c_str(), timestamp_start, timestamp_end));
}

void ArrayContext::delete_fragments_list(
    rust::Slice<const rust::Str> fragment_uris) const {
  std::vector<std::string> c_uris(fragment_uris.size());
  std::vector<const char*> c_uri_ptrs(fragment_uris.size());

  rust_to_cpp(fragment_uris, c_uris, c_uri_ptrs);

  ctx_->handle_error(tiledb_array_delete_fragments_list(
      ctx_->ptr().get(), uri_.c_str(), c_uri_ptrs.data(), c_uri_ptrs.size()));
}

void ArrayContext::vacuum() const {
  ctx_->handle_error(
      tiledb_array_vacuum(ctx_->ptr().get(), uri_.c_str(), nullptr));
}

void ArrayContext::vacuum_with_config(std::shared_ptr<Config> cfg) const {
  ctx_->handle_error(
      tiledb_array_vacuum(ctx_->ptr().get(), uri_.c_str(), cfg->ptr().get()));
}

std::shared_ptr<Schema> ArrayContext::load_schema() const {
  tiledb_array_schema_t* schema;
  ctx_->handle_error(
      tiledb_array_schema_load(ctx_->ptr().get(), uri_.c_str(), &schema));
  return std::make_shared<Schema>(ctx_, schema);
}

std::shared_ptr<Schema> ArrayContext::load_schema_with_config(
    std::shared_ptr<Config> cfg) const {
  tiledb_array_schema_t* schema;
  ctx_->handle_error(tiledb_array_schema_load_with_config(
      ctx_->ptr().get(), cfg->ptr().get(), uri_.c_str(), &schema));
  return std::make_shared<Schema>(ctx_, schema);
}

void ArrayContext::rust_to_cpp(
    rust::Slice<const rust::Str>& rs_uris,
    std::vector<std::string>& c_uris,
    std::vector<const char*>& c_uri_ptrs) const {
  for (auto iter = rs_uris.begin(); iter != rs_uris.end(); ++iter) {
    c_uris.push_back(static_cast<std::string>(*iter));
  }

  for (auto iter = c_uris.begin(); iter != c_uris.end(); ++iter) {
    c_uri_ptrs.push_back(iter->c_str());
  }
}

std::shared_ptr<ArrayContext> create_array_context(
    std::shared_ptr<Context> ctx, rust::Str uri) {
  return std::make_shared<ArrayContext>(ctx, uri);
}

}  // namespace tiledb::rs
