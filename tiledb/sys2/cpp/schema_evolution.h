#ifndef TILEDB_RS_API_SCHEMA_EVOLUTION_H
#define TILEDB_RS_API_SCHEMA_EVOLUTION_H

#include <memory>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Attribute;
class Context;
class CurrentDomain;
class Enumeration;

class SchemaEvolution {
 public:
  SchemaEvolution(const Context& context);
  SchemaEvolution(
      const Context& context, tiledb_array_schema_evolution_t* evolution);

  void add_attribute(const Attribute& attr);
  void drop_attribute(const std::string& attribute_name);

  void add_enumeration(const Enumeration& enmr);
  void extend_enumeration(const Enumeration& enmr);
  void drop_enumeration(const std::string& enumeration_name);

  void expand_current_domain(const CurrentDomain& expanded_domain);

  void set_timestamp_range(
      const std::pair<uint64_t, uint64_t>& timestamp_range);

  void array_evolve(const std::string& array_uri);

  std::shared_ptr<tiledb_array_schema_evolution_t> ptr() const {
    return evolution_;
  }

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_array_schema_evolution_t> evolution_;
};

}  // namespace tiledb::rs

#endif
