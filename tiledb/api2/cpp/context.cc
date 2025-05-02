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

Context::Context(const Config& config) {
  tiledb_ctx_t* ctx;
  if (tiledb_ctx_alloc(config.ptr().get(), &ctx) != TILEDB_OK) {
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

std::unique_ptr<Config> Context::config() const {
  tiledb_config_t* c;
  handle_error(tiledb_ctx_get_config(ctx_.get(), &c));
  return std::make_unique<Config>(c);
}

// ToDo: Create shared filesystem enum

// bool is_supported_fs(tiledb_filesystem_t fs) const;

// void set_tag(const rust::Str key, const rust::Str val);

// rust::String stats() const;

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

}  // namespace tiledb::rs
