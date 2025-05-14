
#ifndef TILEDB_RS_API_SCHEMA_H
#define TILEDB_RS_API_SCHEMA_H

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

#include "rust/cxx.h"
#include "tiledb-sys2/src/array_type.rs.h"
#include "tiledb-sys2/src/layout.rs.h"

namespace tiledb::rs {

class Attribute;
class Context;
class Domain;
class Enumeration;
class FilterList;

class Schema {
 public:
  Schema(std::shared_ptr<Context> ctx, tiledb_array_schema_t* schema);
  Schema(
      std::shared_ptr<Context> ctx,
      std::shared_ptr<tiledb_array_schema_t> schema);

  ArrayType array_type() const;
  uint64_t capacity() const;
  bool allows_dups() const;
  uint32_t version() const;
  TileOrder tile_order() const;
  CellOrder cell_order() const;

  std::shared_ptr<Domain> domain() const;
  uint32_t num_attributes() const;
  bool has_attribute(rust::Str name) const;
  std::shared_ptr<Attribute> attribute_from_name(rust::Str name) const;
  std::shared_ptr<Attribute> attribute_from_index(uint32_t i) const;
  std::shared_ptr<Enumeration> enumeration(rust::Str enmr_name) const;
  std::shared_ptr<Enumeration> enumeration_for_attribute(
      rust::Str attr_name) const;

  std::shared_ptr<FilterList> coords_filter_list() const;
  std::shared_ptr<FilterList> offsets_filter_list() const;
  std::shared_ptr<FilterList> validity_filter_list() const;

  void timestamp_range(uint64_t& start, uint64_t& end) const;

  std::shared_ptr<tiledb_array_schema_t> ptr() const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_array_schema_t> schema_;
};

class SchemaBuilder {
 public:
  SchemaBuilder(std::shared_ptr<Context> ctx, ArrayType type);

  std::shared_ptr<Schema> build() const;

  void set_capacity(uint64_t capacity) const;
  void set_allows_dups(bool allows_dups) const;
  void set_tile_order(TileOrder order) const;
  void set_cell_order(CellOrder order) const;

  void set_domain(std::shared_ptr<Domain> domain) const;
  void add_attribute(std::shared_ptr<Attribute> attr) const;
  void add_enumeration(std::shared_ptr<Enumeration> enmr) const;

  void set_coords_filter_list(std::shared_ptr<FilterList> filters) const;
  void set_offsets_filter_list(std::shared_ptr<FilterList> filters) const;
  void set_validity_filter_list(std::shared_ptr<FilterList> filters) const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_array_schema_t> schema_;
};

std::shared_ptr<SchemaBuilder> create_schema_builder(
    std::shared_ptr<Context> ctx, ArrayType atype);

}  // namespace tiledb::rs

#endif
