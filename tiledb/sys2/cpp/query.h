#ifndef TILEDB_RS_API_QUERY_H
#define TILEDB_RS_API_QUERY_H

#include <array>
#include <string>
#include <unordered_map>
#include <vector>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Array;
class ChannelOperator;
class ChannelOperation;
class Config;
class Context;
class QueryChannel;
class QueryCondition;
class Subarray;

class Query {
 public:
  enum class Status {
    FAILED,
    COMPLETE,
    INPROGRESS,
    INCOMPLETE,
    UNINITIALIZED,
    INITIALIZED
  };

  Query(const Context& ctx, const Array& array);
  Query(const Context& ctx, const Array& array, tiledb_query_type_t type);

  std::shared_ptr<tiledb_query_t> ptr() const;

  tiledb_query_type_t query_type() const;

  tiledb_layout_t query_layout() const;

  const Array& array();
  Status query_status() const;

  bool has_results() const;

  Status submit();

  void finalize();
  void submit_and_finalize();

  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
  result_buffer_elements() const;

  std::unordered_map<std::string, std::tuple<uint64_t, uint64_t, uint64_t>>
  result_buffer_elements_nullable() const;

  uint64_t est_result_size(const std::string& attr_name) const;
  std::array<uint64_t, 2> est_result_size_var(
      const std::string& attr_name) const;

  std::array<uint64_t, 2> est_result_size_nullable(
      const std::string& attr_name) const;

  std::array<uint64_t, 3> est_result_size_var_nullable(
      const std::string& attr_name);

  uint32_t fragment_num() const;

  std::string fragment_uri(uint32_t idx) const;

  std::pair<uint64_t, uint64_t> fragment_timestamp_range(uint32_t idx) const;

  Config config() const;

  void get_data_buffer(
      const std::string& name,
      void** data,
      uint64_t* data_nelements,
      uint64_t* element_size);

  void get_offsets_buffer(
      const std::string& name, uint64_t** offsets, uint64_t* offsets_nelements);

  void get_validity_buffer(
      const std::string& name,
      uint8_t** validity_bytemap,
      uint64_t* validity_bytemap_nelements);

  std::string stats();

  // PJD: I'm pretty sure this is "get subarray of query" and not "update the
  // query's subarray" but I need to track this down to be certain.
  void update_subarray_from_query(Subarray* subarray);

  const Context& ctx() const;
  const Array& array() const;

  void add_update_value_to_query(
      const char* field_name,
      const void* update_value,
      uint64_t update_value_size);

  uint64_t get_relevant_fragment_num();

  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
  result_buffer_elements_labels() const;

  std::unordered_map<std::string, std::tuple<uint64_t, uint64_t, uint64_t>>
  result_buffer_elements_nullable_labels() const;

  QueryChannel get_default_channel();

  template <
      class Op,
      std::enable_if_t<std::is_base_of_v<ChannelOperator, Op>, bool> = true>

  ChannelOperation create_unary_aggregate(const std::string& input_field);

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<Array> array_;
  std::shared_ptr<tiledb_query_t> query_;

  tiledb_datatype_t field_type(const std::string& name) const;
  bool field_var_sized(const std::string& name) const;
};

class QueryBuilder {
 public:
  void set_layout(tiledb_layout_t layout);
  void set_condition(const QueryCondition& condition);
  void set_subarray(const Subarray& subarray);
  void set_config(const Config& config);

  template <typename T>
  void set_data_buffer(const std::string& name, T* buff, uint64_t nelements);

  template <typename T>
  void set_data_buffer(const std::string& name, std::vector<T>& buf);
  void set_data_buffer(const std::string& name, void* buff, uint64_t nelements);
  void set_data_buffer(const std::string& name, std::string& data);

  void set_offsets_buffer(
      const std::string& attr, uint64_t* offsets, uint64_t offset_nelements);

  void set_offsets_buffer(
      const std::string& name, std::vector<uint64_t>& offsets);

  void set_validity_buffer(
      const std::string& attr,
      uint8_t* validity_bytemap,
      uint64_t validity_bytemap_nelements);
  void set_validity_buffer(
      const std::string& name, std::vector<uint8_t>& validity_bytemap);

  // These were all in the experimental header. Not sure why they were
  // duplicated there but leaving them commented out as a note for
  // me to investigate further later.
  //
  // template <typename T>
  // void set_data_buffer(const std::string& name, std::vector<T>& buf);
  // template <typename T>
  // void set_data_buffer(const std::string& name, T* buff, uint64_t nelements);
  // void set_data_buffer(const std::string& name, void* buff, uint64_t
  // nelements); void set_data_buffer(const std::string& name, std::string&
  // data);

 private:
  void set_data_buffer(
      const std::string& attr,
      void* data,
      uint64_t data_nelements,
      size_t data_element_size);
};

}  // namespace tiledb::rs

#endif
