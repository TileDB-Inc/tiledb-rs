#ifndef TILEDB_RS_API_QUERY_CHANNEL_H
#define TILEDB_RS_API_QUERY_CHANNEL_H

#include <memory>

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

namespace tiledb::rs {

class ChannelOperation;
class Context;
class Query;

class QueryChannel {
 public:
  QueryChannel(const Context& ctx, tiledb_query_channel_t* ch);

  void apply_aggregate(
      const std::string& output_field, const ChannelOperation& operation);

 private:
  static QueryChannel create_default_channel(const Query& query);

  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_query_channel_t> channel_;
};

}  // namespace tiledb::rs

#endif
