#ifndef TILEDB_RS_API_QUERY_H
#define TILEDB_RS_API_QUERY_H

#include <tiledb/tiledb.h>

#include "rust/cxx.h"
#include "tiledb-sys2/src/buffer.rs.h"
#include "tiledb-sys2/src/layout.rs.h"
#include "tiledb-sys2/src/mode.rs.h"
#include "tiledb-sys2/src/query_status.rs.h"

namespace tiledb::rs {

class Array;
class ChannelOperator;
class ChannelOperation;
class Config;
class Context;
class QueryChannel;
class QueryCondition;
class Subarray;

struct QueryBufferSizes {
  uint64_t data;
  uint64_t offsets;
  uint64_t validity;
};

class Query {
 public:
  Query(
      std::shared_ptr<Context> ctx,
      std::shared_ptr<Array> array,
      tiledb_query_t* query);

  Query(
      std::shared_ptr<Context> ctx,
      std::shared_ptr<Array> array,
      std::shared_ptr<tiledb_query_t> query);

  Mode mode() const;
  std::shared_ptr<Config> config() const;
  CellOrder layout() const;

  void set_data_buffer(rust::Str name, Buffer& data) const;
  void set_offsets_buffer(rust::Str name, Buffer& offsets) const;
  void set_validity_buffer(rust::Str name, Buffer& validity) const;

  bool get_buffer_sizes(
      rust::Str name,
      uint64_t& data_size,
      uint64_t& offsets_size,
      uint64_t& validity_size) const;

  void submit() const;
  void finalize() const;
  void submit_and_finalize() const;

  QueryStatus status() const;
  bool has_results() const;

  void est_result_size(
      rust::Str name,
      uint64_t& data_size,
      uint64_t& offsets_size,
      uint64_t& validity_size) const;

  uint32_t num_fragments() const;
  uint64_t num_relevant_fragments() const;
  rust::String fragment_uri(uint32_t idx) const;
  void fragment_timestamp_range(uint32_t idx, uint64_t& lo, uint64_t& hi) const;

  rust::String stats() const;

  // PJD: I'm pretty sure this is "get subarray of query" and not "update the
  // query's subarray" but I need to track this down to be certain.
  // void update_subarray_from_query(Subarray* subarray);

  // TODO: Updates
  // void add_update_value_to_query(rust::Str name, Buffer& value) const;

  // ToDo: Dimension Labels
  // std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
  // result_buffer_elements_labels() const;
  // std::unordered_map<std::string, std::tuple<uint64_t, uint64_t, uint64_t>>
  // result_buffer_elements_nullable_labels() const;

  // ToDo: Aggregate queries
  // QueryChannel get_default_channel();
  // ChannelOperation create_unary_aggregate(const std::string& input_field);

  std::shared_ptr<tiledb_query_t> ptr() const;

  std::shared_ptr<QueryBufferSizes> get_sizes(std::string& name) const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<Array> array_;
  std::shared_ptr<tiledb_query_t> query_;
  std::shared_ptr<
      std::unordered_map<std::string, std::shared_ptr<QueryBufferSizes>>>
      sizes_;
};

class QueryBuilder {
 public:
  QueryBuilder(
      std::shared_ptr<Context> ctx, std::shared_ptr<Array> array, Mode mode);

  std::shared_ptr<Query> build() const;

  void set_layout(CellOrder order) const;
  // ToDo: Query Conditions
  // void set_condition(const QueryCondition& condition) const;
  // ToDo: Subarray
  // void set_subarray(const Subarray& subarray) const;
  void set_config(std::shared_ptr<Config> config) const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<Array> array_;
  std::shared_ptr<tiledb_query_t> query_;
};

std::shared_ptr<QueryBuilder> create_query_builder(
    std::shared_ptr<Context> ctx, std::shared_ptr<Array> array, Mode mode);

}  // namespace tiledb::rs

#endif
