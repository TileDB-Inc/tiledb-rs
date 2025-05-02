#ifndef TILEDB_RS_API_CONFIG_H
#define TILEDB_RS_API_CONFIG_H

#include <rust/cxx.h>
#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Config;

std::unique_ptr<Config> create_config();

class Config {
 public:
  Config();
  Config(tiledb_config_t* cfg);

  rust::String get(const rust::Str key) const;
  bool contains(const rust::Str key) const;
  void set(const rust::Str key, const rust::Str val) const;
  void unset(const rust::Str key) const;
  void load_from_file(const rust::Str path) const;
  void save_to_file(const rust::Str path) const;

  std::shared_ptr<tiledb_config_t> ptr() const;

 private:
  std::shared_ptr<tiledb_config_t> config_;

  void create_config();
};

}  // namespace tiledb::rs

#endif  // TILEDB_RS_API_CONFIG_H
