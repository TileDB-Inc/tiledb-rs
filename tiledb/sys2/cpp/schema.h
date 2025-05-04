
#ifndef TILEDB_RS_API_SCHEMA_H
#define TILEDB_RS_API_SCHEMA_H

#include <string>

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

namespace tiledb::rs {

class Attribute;
class Context;
class Domain;
class Enumeration;
class FilterList;

class Schema {
 public:
  Schema(const Context& ctx, const std::string& uri);

  tiledb_array_type_t array_type() const;
  uint64_t capacity() const;
  bool allows_dups() const;
  uint32_t version() const;
  tiledb_layout_t tile_order() const;
  tiledb_layout_t cell_order() const;

  Domain domain() const;
  uint32_t num_attributes() const;
  bool has_attribute(const std::string& name) const;
  Attribute attribute(const std::string& name) const;
  Attribute attribute(unsigned int i) const;
  Enumeration enumeration(const std::string& enmr_name) const;
  Enumeration enumeration_for_attribute(const std::string& attr_name) const;

  FilterList coords_filter_list() const;
  FilterList offsets_filter_list() const;
  FilterList validity_filter_list() const;

  std::pair<uint64_t, uint64_t> timestamp_range() const;
  std::shared_ptr<tiledb_array_schema_t> ptr() const;

 private:
  std::shared_ptr<tiledb_array_schema_t> schema_;
};

class SchemaBuilder {
 public:
  SchemaBuilder(const Context& ctx, tiledb_array_type_t type);

  void set_capacity(uint64_t capacity);
  void set_allows_dups(bool allows_dups);
  void set_tile_order(tiledb_layout_t layout);
  void set_cell_order(tiledb_layout_t layout);

  void set_coords_filter_list(const FilterList& filter_list);
  void set_offsets_filter_list(const FilterList& filter_list);
  void set_validity_filter_list(const FilterList& filter_list);

  void set_domain(const Domain& domain);
  void add_attribute(const Attribute& attr);
  void add_enumeration(const Enumeration& enmr);

  void check() const;
};

}  // namespace tiledb::rs

#endif
