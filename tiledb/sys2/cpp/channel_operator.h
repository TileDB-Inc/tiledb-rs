#ifndef TILEDB_RS_API_CHANNEL_OPERATOR_H
#define TILEDB_RS_API_CHANNEL_OPERATOR_H

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

namespace tiledb::rs {

class ChannelOperator {};

class SumOperator : public ChannelOperator {
 public:
  static const tiledb_channel_operator_t* ptr();
};

class MinOperator : public ChannelOperator {
 public:
  static const tiledb_channel_operator_t* ptr();
};

class MaxOperator : public ChannelOperator {
 public:
  static const tiledb_channel_operator_t* ptr();
};

class MeanOperator : public ChannelOperator {
 public:
  static const tiledb_channel_operator_t* ptr();
};

class NullCountOperator : public ChannelOperator {
 public:
  static const tiledb_channel_operator_t* ptr();
};

}  // namespace tiledb::rs

#endif
