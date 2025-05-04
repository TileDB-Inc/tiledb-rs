#ifndef TILEDB_RS_API_ARRAY_H
#define TILEDB_RS_API_ARRAY_H

#include <memory>
#include <vector>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Config;
class Context;
class Enumeration;
class Schema;

void delete_array(const Context& ctx, const std::string& uri);

void delete_fragments(
    const Context& ctx,
    const std::string& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end);

void delete_fragments_list(
    const Context& ctx,
    const std::string& uri,
    const char* fragment_uris[],
    const size_t num_fragments);

void consolidate(
    const Context& ctx, const std::string& uri, Config* const config = nullptr);

void consolidate(
    const Context& ctx,
    const std::string& array_uri,
    const char* fragment_uris[],
    const size_t num_fragments,
    Config* const config = nullptr);

void consolidate_metadata(
    const Context& ctx, const std::string& uri, Config* const config = nullptr);

void vacuum(
    const Context& ctx, const std::string& uri, Config* const config = nullptr);

void create(const Context& ctx, const std::string& uri, const Schema& schema);

Schema load_schema(const Context& ctx, const std::string& uri);

Schema load_schema_with_config(
    const Context& ctx, const Config& config, const std::string& uri);

tiledb_encryption_type_t encryption_type(
    const Context& ctx, const std::string& array_uri);

void upgrade_version(
    const Context& ctx,
    const std::string& array_uri,
    Config* const config = nullptr);

class Array {
 public:
  Array(std::shared_ptr<const Context> ctx, const std::string& array_uri);

  Array(const Context& ctx, tiledb_array_t* array);

  bool is_open() const;
  std::string uri() const;
  Schema schema() const;
  std::shared_ptr<tiledb_array_t> ptr() const;

  void open(tiledb_query_type_t query_type);
  void open(tiledb_query_type_t query_type, uint64_t end_timestamp);
  void open(
      tiledb_query_type_t query_type,
      tiledb_encryption_type_t encryption_type,
      const std::string& encryption_key);

  void open(
      tiledb_query_type_t query_type,
      tiledb_encryption_type_t encryption_type,
      const std::string& encryption_key,
      uint64_t end_timestamp);

  void reopen();

  void set_open_timestamp_start(uint64_t timestamp_start) const;
  void set_open_timestamp_end(uint64_t timestamp_end) const;
  uint64_t open_timestamp_start() const;
  uint64_t open_timestamp_end() const;

  void set_config(const Config& config) const;
  Config config() const;
  void close();

  Enumeration get_enumeration(const std::string& attr_name);

  void load_all_enumerations(const Context& ctx, const Array& array);
  void load_enumerations_all_schemas(const Context& ctx, const Array& array);

  template <typename T>
  std::vector<std::pair<std::string, std::pair<T, T>>> non_empty_domain();

  template <typename T>
  std::pair<T, T> non_empty_domain(unsigned idx);

  template <typename T>
  std::pair<T, T> non_empty_domain(const std::string& name);

  std::pair<std::string, std::string> non_empty_domain_var(unsigned idx);

  std::pair<std::string, std::string> non_empty_domain_var(
      const std::string& name);

  tiledb_query_type_t query_type() const;

  void put_metadata(
      const std::string& key,
      tiledb_datatype_t value_type,
      uint32_t value_num,
      const void* value);

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

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_array_t> array_;
};

}  // namespace tiledb::rs

#endif
