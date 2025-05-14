
#ifndef TILEDB_RS_API_ATTRIBUTE_H
#define TILEDB_RS_API_ATTRIBUTE_H

#include <string>

#include <tiledb/tiledb.h>

#include "rust/cxx.h"
#include "tiledb-sys2/src/buffer.rs.h"
#include "tiledb-sys2/src/datatype.rs.h"

namespace tiledb::rs {

class Context;
class FilterList;

class Attribute {
 public:
  Attribute(std::shared_ptr<Context> ctx, tiledb_attribute_t* attr);
  Attribute(
      std::shared_ptr<Context> ctx, std::shared_ptr<tiledb_attribute_t> attr);

  rust::String name() const;
  Datatype datatype() const;
  uint64_t cell_size() const;
  uint32_t cell_val_num() const;
  bool nullable() const;
  bool enumeration_name(rust::String& name) const;
  std::shared_ptr<FilterList> filter_list() const;

  uint64_t fill_value_size() const;
  void fill_value(Buffer& buf) const;
  void fill_value_nullable(Buffer& buf, uint8_t& validity) const;

  std::shared_ptr<tiledb_attribute_t> ptr() const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_attribute_t> attr_;
};

class AttributeBuilder {
 public:
  AttributeBuilder(
      std::shared_ptr<Context> ctx, std::string name, tiledb_datatype_t type);

  std::shared_ptr<Attribute> build() const;

  void set_nullable(bool nullable) const;
  void set_cell_val_num(uint32_t num) const;
  void set_enumeration_name(const rust::Str enumeration_name) const;
  void set_filter_list(std::shared_ptr<FilterList> filter_list) const;
  void set_fill_value(Buffer& value) const;
  void set_fill_value_nullable(Buffer& value, uint8_t valid) const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_attribute_t> attr_;
};

std::shared_ptr<AttributeBuilder> create_attribute_builder(
    std::shared_ptr<Context> ctx, rust::Str name, Datatype dtype);

}  // namespace tiledb::rs

#endif
