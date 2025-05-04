#ifndef TILEDB_RS_API_OBJECT_H
#define TILEDB_RS_API_OBJECT_H

#include <optional>
#include <string>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Context;

void remove_object(const Context& ctx, const std::string& uri);

void move_object(
    const Context& ctx, const std::string& old_uri, const std::string& new_uri);

class Object {
 public:
  enum class Type { Array, Group, Invalid };

  Object(const Context& ctx, const std::string& uri);

  Object(
      const Type& type,
      const std::string& uri = "",
      const std::optional<std::string>& name = std::nullopt);

  Object(
      tiledb_object_t type,
      const std::string& uri = "",
      const std::optional<std::string>& name = std::nullopt);

  Type type() const;
  std::string uri() const;
  std::optional<std::string> name() const;

 private:
  Type type_;
  std::string uri_;
  std::optional<std::string> name_;
};

}  // namespace tiledb::rs

#endif
