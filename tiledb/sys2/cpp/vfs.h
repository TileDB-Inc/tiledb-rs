#ifndef TILEDB_RS_API_VFS_H
#define TILEDB_RS_API_VFS_H

#include <functional>
#include <string>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Config;
class Context;

using LsCallback = std::function<bool(std::string_view, uint64_t)>;
using LsInclude = std::function<bool(std::string_view, uint64_t)>;
using LsObjects = std::vector<std::pair<std::string, uint64_t>>;

class VFS {
 public:
  VFS(const Context& ctx);
  VFS(const Context& ctx, const Config& config);

  void create_bucket(const std::string& uri) const;
  void remove_bucket(const std::string& uri) const;
  bool is_bucket(const std::string& uri) const;
  void empty_bucket(const std::string& bucket) const;
  bool is_empty_bucket(const std::string& bucket) const;

  void create_dir(const std::string& uri) const;
  bool is_dir(const std::string& uri) const;
  void remove_dir(const std::string& uri) const;

  bool is_file(const std::string& uri) const;
  void remove_file(const std::string& uri) const;
  uint64_t dir_size(const std::string& uri) const;

  std::vector<std::string> ls(const std::string& uri) const;

  uint64_t file_size(const std::string& uri) const;
  void move_file(const std::string& old_uri, const std::string& new_uri);

  void move_dir(const std::string& old_uri, const std::string& new_uri) const;

  void copy_file(const std::string& old_uri, const std::string& new_uri) const;

  void copy_dir(const std::string& old_uri, const std::string& new_uri) const;

  void touch(const std::string& uri) const;

  const Context& context() const;

  std::shared_ptr<tiledb_vfs_t> ptr() const;

  Config config() const;

  void ls_recursive(const std::string& uri, LsCallback cb);

  LsObjects ls_recursive_filter(
      const std::string& uri, std::optional<LsInclude> include = std::nullopt);

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_vfs_t> vfs_;

  void create_vfs(tiledb_config_t* config);
};
}  // namespace tiledb::rs

#endif
