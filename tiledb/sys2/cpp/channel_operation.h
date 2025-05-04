#ifndef TILEDB_RS_API_CHANNEL_OPERATION_H
#define TILEDB_RS_API_CHANNEL_OPERATION_H

#include <string>
#include <type_traits>

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

namespace tiledb::rs {

class ChannelOperator;
class Context;
class Query;

class ChannelOperation {
 public:
  ChannelOperation() = default;
  ChannelOperation(const Context& ctx, tiledb_channel_operation_t* operation);

 private:
  virtual const tiledb_channel_operation_t* ptr() const;

  template <
      class Op,
      std::enable_if_t<std::is_base_of_v<ChannelOperator, Op>, bool> = true>
  static ChannelOperation create(
      const Query& query, const std::string& input_field);

  std::shared_ptr<tiledb_channel_operation_t> operation_;
};

class CountOperation : public ChannelOperation {
 private:
  virtual const tiledb_channel_operation_t* ptr() const {
    return tiledb_aggregate_count;
  }
};

}  // namespace tiledb::rs

#endif
