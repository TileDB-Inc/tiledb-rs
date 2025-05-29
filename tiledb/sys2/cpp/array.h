#ifndef TILEDB_RS_API_ARRAY_H
#define TILEDB_RS_API_ARRAY_H

#include <memory>

#include <tiledb/tiledb.h>

#include "rust/cxx.h"
#include "tiledb-sys2/src/buffer.rs.h"
#include "tiledb-sys2/src/datatype.rs.h"
#include "tiledb-sys2/src/mode.rs.h"

namespace tiledb::rs {

class Config;
class Context;
class Enumeration;
class Schema;

class Array {
 public:
  Array(std::shared_ptr<Context> ctx, tiledb_array_t* array);
  Array(std::shared_ptr<Context> ctx, rust::Str array_uri);

  rust::String uri() const;

  void set_config(std::shared_ptr<Config> config) const;
  void set_open_timestamp_start(uint64_t timestamp_start) const;
  void set_open_timestamp_end(uint64_t timestamp_end) const;

  void open(Mode mode) const;
  void reopen() const;
  void close() const;

  bool is_open() const;
  Mode mode() const;
  std::shared_ptr<Config> config() const;
  std::shared_ptr<Schema> schema() const;
  uint64_t open_timestamp_start() const;
  uint64_t open_timestamp_end() const;

  std::shared_ptr<Enumeration> get_enumeration(rust::Str enmr_name) const;
  void load_all_enumerations() const;
  void load_enumerations_all_schemas() const;

  bool non_empty_domain_from_index(uint32_t index, Buffer& buffer) const;
  bool non_empty_domain_from_name(rust::Str name, Buffer& buffer) const;

  bool non_empty_domain_var_from_index(
      uint32_t index, Buffer& lower, Buffer& upper) const;
  bool non_empty_domain_var_from_name(
      rust::Str name, Buffer& lower, Buffer& upper) const;

  void put_metadata(
      rust::Str key, Datatype dtype, uint32_t num, Buffer& value) const;

  void get_metadata(rust::Str key, Datatype& dtype, Buffer& value) const;

  void delete_metadata(rust::Str key) const;

  bool has_metadata(rust::Str key, Datatype& dtype) const;

  uint64_t num_metadata() const;

  void get_metadata_from_index(
      uint64_t index,
      rust::Vec<uint8_t>& key,
      Datatype& dtype,
      Buffer& values) const;

  std::shared_ptr<tiledb_array_t> ptr() const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_array_t> array_;
};

std::shared_ptr<Array> create_array(
    std::shared_ptr<Context> ctx, rust::Str uri);

class ArrayContext {
 public:
  ArrayContext(std::shared_ptr<Context> ctx, rust::Str uri);

  void create(std::shared_ptr<Schema> schema) const;
  void destroy() const;

  void consolidate() const;
  void consolidate_with_config(std::shared_ptr<Config> cfg) const;

  void consolidate_list(rust::Slice<const rust::Str> fragment_uris) const;
  void consolidate_list_with_config(
      rust::Slice<const rust::Str> fragment_uris,
      std::shared_ptr<Config> cfg) const;

  void consolidate_metadata() const;
  void consolidate_metadata_with_config(std::shared_ptr<Config> cfg) const;

  void delete_fragments(uint64_t timestamp_start, uint64_t timestamp_end) const;

  void delete_fragments_list(rust::Slice<const rust::Str> fragment_uris) const;

  void vacuum() const;
  void vacuum_with_config(std::shared_ptr<Config> cfg) const;

  std::shared_ptr<Schema> load_schema() const;
  std::shared_ptr<Schema> load_schema_with_config(
      std::shared_ptr<Config> cfg) const;

  void rust_to_cpp(
      rust::Slice<const rust::Str>& rs_uris,
      std::vector<std::string>& c_strs,
      std::vector<const char*>& c_str_pts) const;

 private:
  std::shared_ptr<Context> ctx_;
  std::string uri_;
};

std::shared_ptr<ArrayContext> create_array_context(
    std::shared_ptr<Context> ctx, rust::Str uri);

}  // namespace tiledb::rs

#endif
