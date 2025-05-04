#ifndef TILEDB_RS_API_STATS_H
#define TILEDB_RS_API_STATS_H

#include <string>

namespace tiledb::rs {

class Stats {
 public:
  static void enable();
  static void disable();
  static bool is_enabled();
  static void reset();
  static std::string dump();
  static std::string raw_dump();
};

}  // namespace tiledb::rs

#endif
