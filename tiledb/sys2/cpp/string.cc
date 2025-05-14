#include <memory>

#include <tiledb/tiledb_experimental.h>

#include "string.h"

namespace tiledb::rs {

static void delete_string(tiledb_string_t* str) {
  if (str != nullptr) {
    tiledb_string_free(&str);
  }
}

String::String(tiledb_string_t* str)
    : str_(str, delete_string) {
}

std::string_view String::view() const {
  const char* data = nullptr;
  size_t length = 0;

  tiledb_string_view(str_.get(), &data, &length);

  return std::string_view(data, length);
}

}  // namespace tiledb::rs
