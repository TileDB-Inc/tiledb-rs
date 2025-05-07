#ifndef TILEDB_RS_API_CONTEXT_H
#define TILEDB_RS_API_CONTEXT_H

#include <rust/cxx.h>
#include <tiledb/tiledb.h>

#include "config.h"

namespace tiledb::rs {

class Context {
 public:
  Context();
  Context(const std::shared_ptr<Config>& config);

  std::shared_ptr<Config> config() const;

  void handle_error(int rc) const;

  bool is_supported_fs(int32_t fs) const;

  void set_tag(const rust::Str key, const rust::Str val) const;

  rust::String stats() const;

  std::shared_ptr<tiledb_ctx_t> ptr() const;

 private:
  std::string get_last_error_message() const noexcept;

  std::shared_ptr<tiledb_ctx_t> ctx_;
};

std::shared_ptr<Context> create_context();
std::shared_ptr<Context> create_context_with_config(
    const std::shared_ptr<Config>& cfg);

}  // namespace tiledb::rs

#endif
