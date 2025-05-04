#ifndef TILEDB_RS_API_QUERY_CONDITION_H
#define TILEDB_RS_API_QUERY_CONDITION_H

#include <memory>
#include <vector>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Context;

class QueryCondition {
 public:
  QueryCondition(const Context& ctx);
  QueryCondition(const Context& ctx, tiledb_query_condition_t* const qc);

  void init(
      const std::string& attribute_name,
      const void* condition_value,
      uint64_t condition_value_size,
      tiledb_query_condition_op_t op);

  void init(
      const std::string& attribute_name,
      const std::string& condition_value,
      tiledb_query_condition_op_t op);

  std::shared_ptr<tiledb_query_condition_t> ptr() const;

  QueryCondition combine(
      const QueryCondition& rhs,
      tiledb_query_condition_combination_op_t combination_op) const;

  QueryCondition negate() const;

  static QueryCondition create(
      const Context& ctx,
      const std::string& attribute_name,
      const std::string& value,
      tiledb_query_condition_op_t op);

  template <typename T>
  static QueryCondition create(
      const Context& ctx,
      const std::string& attribute_name,
      T value,
      tiledb_query_condition_op_t op);

  template <typename T>
  static QueryCondition create(
      const Context& ctx,
      const std::string& field_name,
      const std::vector<T>& values,
      tiledb_query_condition_op_t op);

  template <typename T>
  static QueryCondition create(
      const Context& ctx,
      const std::string& field_name,
      const std::vector<std::basic_string<T>>& values,
      tiledb_query_condition_op_t op);

  void set_use_enumeration(bool use_enumeration);

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_query_condition_t> query_condition_;
};

}  // namespace tiledb::rs

#endif
