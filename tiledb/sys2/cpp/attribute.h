
#ifndef TILEDB_RS_API_ATTRIBUTE_H
#define TILEDB_RS_API_ATTRIBUTE_H

#include <string>

#include <tiledb/tiledb.h>

#include "rust/cxx.h"
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

  void get_fill_value(const void** value, uint64_t* size) const;
  void get_fill_value(const void** value, uint64_t* size, uint8_t* valid) const;

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

  void set_fill_value_i8(rust::Slice<const int8_t> value) const;
  void set_fill_value_i16(rust::Slice<const int16_t> value) const;
  void set_fill_value_i32(rust::Slice<const int32_t> value) const;
  void set_fill_value_i64(rust::Slice<const int64_t> value) const;
  void set_fill_value_u8(rust::Slice<const uint8_t> value) const;
  void set_fill_value_u16(rust::Slice<const uint16_t> value) const;
  void set_fill_value_u32(rust::Slice<const uint32_t> value) const;
  void set_fill_value_u64(rust::Slice<const uint64_t> value) const;
  void set_fill_value_f32(rust::Slice<const float> value) const;
  void set_fill_value_f64(rust::Slice<const double> value) const;

  template <typename T>
  void set_fill_value(const rust::Slice<const T> value) const;

  void set_fill_value_nullable_i8(
      rust::Slice<const int8_t> value, uint8_t valid) const;
  void set_fill_value_nullable_i16(
      rust::Slice<const int16_t> value, uint8_t valid) const;
  void set_fill_value_nullable_i32(
      rust::Slice<const int32_t> value, uint8_t valid) const;
  void set_fill_value_nullable_i64(
      rust::Slice<const int64_t> value, uint8_t valid) const;
  void set_fill_value_nullable_u8(
      rust::Slice<const uint8_t> value, uint8_t valid) const;
  void set_fill_value_nullable_u16(
      rust::Slice<const uint16_t> value, uint8_t valid) const;
  void set_fill_value_nullable_u32(
      rust::Slice<const uint32_t> value, uint8_t valid) const;
  void set_fill_value_nullable_u64(
      rust::Slice<const uint64_t> value, uint8_t valid) const;
  void set_fill_value_nullable_f32(
      rust::Slice<const float> value, uint8_t valid) const;
  void set_fill_value_nullable_f64(
      rust::Slice<const double> value, uint8_t valid) const;

  template <typename T>
  void set_fill_value_nullable(rust::Slice<const T>, uint8_t valid) const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_attribute_t> attr_;
};

std::shared_ptr<AttributeBuilder> create_attribute_builder(
    std::shared_ptr<Context> ctx, rust::Str name, Datatype dtype);

}  // namespace tiledb::rs

#endif
