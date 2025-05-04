#ifndef TILEDB_RS_API_GROUP_H
#define TILEDB_RS_API_GROUP_H

#include <memory>
#include <optional>
#include <string>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Config;
class Context;
class Object;

void create_group(const Context& ctx, const std::string& uri);

void consolidate_metadata(
    const Context& ctx, const std::string& uri, Config* const config = nullptr);

void vacuum_metadata(
    const Context& ctx, const std::string& uri, Config* const config = nullptr);

class Group {
 public:
  Group(
      const Context& ctx,
      const std::string& group_uri,
      tiledb_query_type_t query_type);

  Group(
      const Context& ctx,
      const std::string& group_uri,
      tiledb_query_type_t query_type,
      const Config& config);

  void open(tiledb_query_type_t query_type);

  void set_config(const Config& config) const;

  Config config() const;

  void close(bool should_throw = true);

  bool is_open() const;

  std::string uri() const;

  tiledb_query_type_t query_type() const;

  void put_metadata(
      const std::string& key,
      tiledb_datatype_t value_type,
      uint32_t value_num,
      const void* value);

  void delete_group(const std::string& uri, bool recursive = false);

  void delete_metadata(const std::string& key);

  void get_metadata(
      const std::string& key,
      tiledb_datatype_t* value_type,
      uint32_t* value_num,
      const void** value);

  bool has_metadata(const std::string& key, tiledb_datatype_t* value_type);

  uint64_t metadata_num() const;

  void get_metadata_from_index(
      uint64_t index,
      std::string* key,
      tiledb_datatype_t* value_type,
      uint32_t* value_num,
      const void** value);

  void add_member(
      const std::string& uri,
      const bool& relative,
      std::optional<std::string> name = std::nullopt,
      std::optional<tiledb_object_t> type = std::nullopt);

  void remove_member(const std::string& name_or_uri);

  uint64_t member_count() const;

  Object member(uint64_t index) const;

  Object member(std::string name) const;

  bool is_relative(std::string name) const;

  std::string dump(const bool recursive) const;

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_group_t> group_;
};

}  // namespace tiledb::rs

#endif
