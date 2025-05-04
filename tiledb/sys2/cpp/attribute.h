
#ifndef TILEDB_RS_API_ATTRIBUTE_H
#define TILEDB_RS_API_ATTRIBUTE_H

#include <string>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Context;
class FilterList;

class Attribute {
 public:
  Attribute(const Context& ctx, tiledb_attribute_t* attr);

  std::string name() const;
  const Context& context() const;
  tiledb_datatype_t type() const;

  uint64_t cell_size() const;
  unsigned cell_val_num() const;
  bool nullable() const;
  std::optional<std::string> enumeration_name() const;

  void get_fill_value(const void** value, uint64_t* size) const;
  void get_fill_value(const void** value, uint64_t* size, uint8_t* valid) const;

  FilterList filter_list() const;

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_attribute_t> attr_;
};

class AttributeBuilder {
 public:
  AttributeBuilder(
      const Context& ctx, const std::string& name, tiledb_datatype_t type);

  void set_nullable(bool nullable);
  void set_cell_val_num(unsigned num);
  void set_enumeration_name(const std::string& enumeration_name);
  void set_filter_list(const FilterList& filter_list);
  void set_fill_value(const void* value, uint64_t size);
  void set_fill_value(const void* value, uint64_t size, uint8_t valid);
};

}  // namespace tiledb::rs

#endif
