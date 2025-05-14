#ifndef TILEDB_RS_API_STRING_H
#define TILEDB_RS_API_STRING_H

#include <string_view>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Context;

class String {
 public:
  String(tiledb_string_t* str);

  std::string_view view() const;

 private:
  std::shared_ptr<tiledb_string_t> str_;
};

}  // namespace tiledb::rs

#endif
