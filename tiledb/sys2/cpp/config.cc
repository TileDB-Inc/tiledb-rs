#include "config.h"
#include "exception.h"

namespace tiledb::rs {

static void config_free(tiledb_config_t* config) {
  tiledb_config_free(&config);
}

inline void check_config_error(tiledb_error_t* err) {
  if (err != nullptr) {
    const char* msg_cstr;
    tiledb_error_message(err, &msg_cstr);
    std::string msg = "Config Error: " + std::string(msg_cstr);
    tiledb_error_free(&err);
    throw TileDBError(msg);
  }
}

Config::Config() {
  create_config();
}

Config::Config(tiledb_config_t* cfg) {
  assert(cfg != nullptr);
  config_ = std::shared_ptr<tiledb_config_t>(cfg, config_free);
}

rust::String Config::get(const rust::Str key) const {
  auto c_key = static_cast<std::string>(key);

  const char* val;
  tiledb_error_t* err;
  tiledb_config_get(config_.get(), c_key.c_str(), &val, &err);
  check_config_error(err);

  if (val == nullptr)
    throw TileDBError("Config Error: Invalid parameter '" + c_key + "'");

  return val;
}

bool Config::contains(const rust::Str key) const {
  auto c_key = static_cast<std::string>(key);
  const char* val;
  tiledb_error_t* err;
  tiledb_config_get(config_.get(), c_key.data(), &val, &err);

  return val != nullptr;
}

void Config::set(const rust::Str key, const rust::Str val) const {
  auto c_key = static_cast<std::string>(key);
  auto c_val = static_cast<std::string>(val);
  tiledb_error_t* err;
  tiledb_config_set(config_.get(), c_key.c_str(), c_val.c_str(), &err);
  check_config_error(err);
}

void Config::unset(const rust::Str key) const {
  auto c_key = static_cast<std::string>(key);
  tiledb_error_t* err;
  tiledb_config_unset(config_.get(), c_key.c_str(), &err);
  check_config_error(err);
}

void Config::load_from_file(const rust::Str path) const {
  auto c_path = static_cast<std::string>(path);
  tiledb_error_t* err;
  tiledb_config_load_from_file(config_.get(), c_path.c_str(), &err);
  check_config_error(err);
}

void Config::save_to_file(const rust::Str path) const {
  auto c_path = static_cast<std::string>(path);
  tiledb_error_t* err;
  tiledb_config_save_to_file(config_.get(), c_path.c_str(), &err);
  check_config_error(err);
}

std::shared_ptr<tiledb_config_t> Config::ptr() const {
  return config_;
}

void Config::create_config() {
  tiledb_config_t* config;
  tiledb_error_t* err;
  tiledb_config_alloc(&config, &err);
  check_config_error(err);

  config_ = std::shared_ptr<tiledb_config_t>(config, config_free);
}

std::shared_ptr<Config> create_config() {
  return std::make_shared<Config>();
}

}  // namespace tiledb::rs
