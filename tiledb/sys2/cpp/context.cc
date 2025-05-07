#include "context.h"
#include "exception.h"

static void context_free(tiledb_ctx_t* config) {
  tiledb_ctx_free(&config);
}

namespace tiledb::rs {

Context::Context() {
  tiledb_ctx_t* ctx;
  if (tiledb_ctx_alloc(nullptr, &ctx) != TILEDB_OK) {
    throw TileDBError("[TileDB::C++API] Error: Failed to create context");
  }

  ctx_ = std::shared_ptr<tiledb_ctx_t>(ctx, context_free);

  set_tag("x-tiledb-api-language", "Rust");
}

Context::Context(const std::shared_ptr<Config>& config) {
  tiledb_ctx_t* ctx;
  if (tiledb_ctx_alloc(config->ptr().get(), &ctx) != TILEDB_OK) {
    throw TileDBError("[TileDB::C++API] Error: Failed to create context");
  }

  ctx_ = std::shared_ptr<tiledb_ctx_t>(ctx, context_free);

  set_tag("x-tiledb-api-language", "c++");
}

void Context::handle_error(int rc) const {
  if (rc == TILEDB_OK) {
    return;
  }

  throw TileDBError(get_last_error_message());
}

std::shared_ptr<Config> Context::config() const {
  tiledb_config_t* c;
  handle_error(tiledb_ctx_get_config(ctx_.get(), &c));
  return std::make_shared<Config>(c);
}

bool Context::is_supported_fs(int32_t fs) const {
  tiledb_filesystem_t cpp_fs;
  if (fs == 0) {
    cpp_fs = TILEDB_HDFS;
  } else if (fs == 1) {
    cpp_fs = TILEDB_S3;
  } else if (fs == 2) {
    cpp_fs = TILEDB_AZURE;
  } else if (fs == 3) {
    cpp_fs = TILEDB_GCS;
  } else if (fs == 4) {
    cpp_fs = TILEDB_MEMFS;
  } else {
    throw TileDBError("Invalid filesystem variant.");
  }

  int ret;
  handle_error(tiledb_ctx_is_supported_fs(ctx_.get(), cpp_fs, &ret));
  return ret != 0;
}

void Context::set_tag(const rust::Str key, const rust::Str val) const {
  auto c_key = static_cast<std::string>(key);
  auto c_val = static_cast<std::string>(val);

  handle_error(tiledb_ctx_set_tag(ctx_.get(), c_key.c_str(), c_val.c_str()));
}

rust::String Context::stats() const {
  char* c_str;
  handle_error(tiledb_ctx_get_stats(ctx_.get(), &c_str));

  std::string str(c_str);
  tiledb_stats_free_str(&c_str);

  return str;
}

std::shared_ptr<tiledb_ctx_t> Context::ptr() const {
  return ctx_;
}

std::string Context::get_last_error_message() const noexcept {
  // Get error
  const auto& ctx = ctx_.get();
  tiledb_error_t* err = nullptr;

  auto rc = tiledb_ctx_get_last_error(ctx, &err);
  if (rc != TILEDB_OK) {
    tiledb_error_free(&err);
    return "[TileDB::C++API] Error: Non-retrievable error occurred";
  }

  // Get the error message
  const char* msg = nullptr;
  rc = tiledb_error_message(err, &msg);
  if (rc != TILEDB_OK) {
    tiledb_error_free(&err);
    return "[TileDB::C++API] Error: Non-retrievable error occurred";
  }

  // Create a copy of the error
  std::string msg_str(msg);

  // Cleanup error struct
  tiledb_error_free(&err);

  return msg_str;
}

std::shared_ptr<Context> create_context() {
  return std::make_shared<Context>();
}

std::shared_ptr<Context> create_context_with_config(
    const std::shared_ptr<Config>& cfg) {
  return std::make_shared<Context>(cfg);
}

}  // namespace tiledb::rs
