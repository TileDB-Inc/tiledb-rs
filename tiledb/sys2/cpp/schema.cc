
#include "schema.h"
#include "array_type.h"
#include "attribute.h"
#include "context.h"
#include "domain.h"
#include "enumeration.h"
#include "filter_list.h"
#include "layout.h"

namespace tiledb::rs {

static void delete_schema(tiledb_array_schema_t* schema) {
  if (schema != nullptr) {
    tiledb_array_schema_free(&schema);
  }
}

Schema::Schema(std::shared_ptr<Context> ctx, tiledb_array_schema_t* schema)
    : ctx_(ctx)
    , schema_(schema, delete_schema) {
}

Schema::Schema(
    std::shared_ptr<Context> ctx, std::shared_ptr<tiledb_array_schema_t> schema)
    : ctx_(ctx)
    , schema_(schema) {
}

ArrayType Schema::array_type() const {
  tiledb_array_type_t atype;
  ctx_->handle_error(tiledb_array_schema_get_array_type(
      ctx_->ptr().get(), schema_.get(), &atype));
  return to_rs_array_type(atype);
}

uint64_t Schema::capacity() const {
  uint64_t capacity;
  ctx_->handle_error(tiledb_array_schema_get_capacity(
      ctx_->ptr().get(), schema_.get(), &capacity));
  return capacity;
}

bool Schema::allows_dups() const {
  int allows_dups;
  ctx_->handle_error(tiledb_array_schema_get_allows_dups(
      ctx_->ptr().get(), schema_.get(), &allows_dups));
  return allows_dups != 0;
}

uint32_t Schema::version() const {
  uint32_t version;
  ctx_->handle_error(tiledb_array_schema_get_version(
      ctx_->ptr().get(), schema_.get(), &version));
  return version;
}

TileOrder Schema::tile_order() const {
  tiledb_layout_t order;
  ctx_->handle_error(tiledb_array_schema_get_tile_order(
      ctx_->ptr().get(), schema_.get(), &order));
  return to_rs_tile_order(order);
}

CellOrder Schema::cell_order() const {
  tiledb_layout_t order;
  ctx_->handle_error(tiledb_array_schema_get_cell_order(
      ctx_->ptr().get(), schema_.get(), &order));
  return to_rs_cell_order(order);
}

std::shared_ptr<Domain> Schema::domain() const {
  tiledb_domain_t* dom = nullptr;
  ctx_->handle_error(
      tiledb_array_schema_get_domain(ctx_->ptr().get(), schema_.get(), &dom));
  return std::make_shared<Domain>(ctx_, dom);
}

uint32_t Schema::num_attributes() const {
  uint32_t num;
  ctx_->handle_error(tiledb_array_schema_get_attribute_num(
      ctx_->ptr().get(), schema_.get(), &num));
  return num;
}

bool Schema::has_attribute(rust::Str name) const {
  auto c_name = static_cast<std::string>(name);
  int32_t exists;
  ctx_->handle_error(tiledb_array_schema_has_attribute(
      ctx_->ptr().get(), schema_.get(), c_name.c_str(), &exists));
  return exists != 0;
}

std::shared_ptr<Attribute> Schema::attribute_from_name(rust::Str name) const {
  auto c_name = static_cast<std::string>(name);
  tiledb_attribute_t* attr = nullptr;
  ctx_->handle_error(tiledb_array_schema_get_attribute_from_name(
      ctx_->ptr().get(), schema_.get(), c_name.c_str(), &attr));
  return std::make_shared<Attribute>(ctx_, attr);
}

std::shared_ptr<Attribute> Schema::attribute_from_index(uint32_t index) const {
  tiledb_attribute_t* attr = nullptr;
  ctx_->handle_error(tiledb_array_schema_get_attribute_from_index(
      ctx_->ptr().get(), schema_.get(), index, &attr));
  return std::make_shared<Attribute>(ctx_, attr);
}

std::shared_ptr<Enumeration> Schema::enumeration(rust::Str enmr_name) const {
  auto c_name = static_cast<std::string>(enmr_name);
  tiledb_enumeration_t* enmr = nullptr;
  ctx_->handle_error(tiledb_array_schema_get_enumeration_from_name(
      ctx_->ptr().get(), schema_.get(), c_name.c_str(), &enmr));
  return std::make_shared<Enumeration>(ctx_, enmr);
}

std::shared_ptr<Enumeration> Schema::enumeration_for_attribute(
    rust::Str attr_name) const {
  auto c_name = static_cast<std::string>(attr_name);
  tiledb_enumeration_t* enmr = nullptr;
  ctx_->handle_error(tiledb_array_schema_get_enumeration_from_attribute_name(
      ctx_->ptr().get(), schema_.get(), c_name.c_str(), &enmr));
  return std::make_shared<Enumeration>(ctx_, enmr);
}

std::shared_ptr<FilterList> Schema::coords_filter_list() const {
  tiledb_filter_list_t* list = nullptr;
  ctx_->handle_error(tiledb_array_schema_get_coords_filter_list(
      ctx_->ptr().get(), schema_.get(), &list));
  return std::make_shared<FilterList>(ctx_, list);
}

std::shared_ptr<FilterList> Schema::offsets_filter_list() const {
  tiledb_filter_list_t* list = nullptr;
  ctx_->handle_error(tiledb_array_schema_get_offsets_filter_list(
      ctx_->ptr().get(), schema_.get(), &list));
  return std::make_shared<FilterList>(ctx_, list);
}

std::shared_ptr<FilterList> Schema::validity_filter_list() const {
  tiledb_filter_list_t* list = nullptr;
  ctx_->handle_error(tiledb_array_schema_get_validity_filter_list(
      ctx_->ptr().get(), schema_.get(), &list));
  return std::make_shared<FilterList>(ctx_, list);
}

void Schema::timestamp_range(uint64_t& start, uint64_t& end) const {
  ctx_->handle_error(tiledb_array_schema_timestamp_range(
      ctx_->ptr().get(), schema_.get(), &start, &end));
}

std::shared_ptr<tiledb_array_schema_t> Schema::ptr() const {
  return schema_;
}

SchemaBuilder::SchemaBuilder(std::shared_ptr<Context> ctx, ArrayType atype)
    : ctx_(ctx) {
  auto c_atype = to_cpp_array_type(atype);
  tiledb_array_schema_t* schema = nullptr;
  ctx_->handle_error(
      tiledb_array_schema_alloc(ctx_->ptr().get(), c_atype, &schema));
  schema_ = std::shared_ptr<tiledb_array_schema_t>(schema, delete_schema);
}

std::shared_ptr<Schema> SchemaBuilder::build() const {
  ctx_->handle_error(
      tiledb_array_schema_check(ctx_->ptr().get(), schema_.get()));
  return std::make_shared<Schema>(ctx_, schema_);
}

void SchemaBuilder::set_capacity(uint64_t capacity) const {
  ctx_->handle_error(tiledb_array_schema_set_capacity(
      ctx_->ptr().get(), schema_.get(), capacity));
}

void SchemaBuilder::set_allows_dups(bool allows_dups) const {
  ctx_->handle_error(tiledb_array_schema_set_allows_dups(
      ctx_->ptr().get(), schema_.get(), allows_dups));
}

void SchemaBuilder::set_tile_order(TileOrder order) const {
  auto c_order = to_cpp_tile_order(order);
  ctx_->handle_error(tiledb_array_schema_set_tile_order(
      ctx_->ptr().get(), schema_.get(), c_order));
}

void SchemaBuilder::set_cell_order(CellOrder order) const {
  auto c_order = to_cpp_cell_order(order);
  ctx_->handle_error(tiledb_array_schema_set_cell_order(
      ctx_->ptr().get(), schema_.get(), c_order));
}

void SchemaBuilder::set_domain(std::shared_ptr<Domain> domain) const {
  ctx_->handle_error(tiledb_array_schema_set_domain(
      ctx_->ptr().get(), schema_.get(), domain->ptr().get()));
}

void SchemaBuilder::add_attribute(std::shared_ptr<Attribute> attr) const {
  ctx_->handle_error(tiledb_array_schema_add_attribute(
      ctx_->ptr().get(), schema_.get(), attr->ptr().get()));
}

void SchemaBuilder::add_enumeration(std::shared_ptr<Enumeration> enmr) const {
  ctx_->handle_error(tiledb_array_schema_add_enumeration(
      ctx_->ptr().get(), schema_.get(), enmr->ptr().get()));
}

void SchemaBuilder::set_coords_filter_list(
    std::shared_ptr<FilterList> filters) const {
  ctx_->handle_error(tiledb_array_schema_set_coords_filter_list(
      ctx_->ptr().get(), schema_.get(), filters->ptr().get()));
}

void SchemaBuilder::set_offsets_filter_list(
    std::shared_ptr<FilterList> filters) const {
  ctx_->handle_error(tiledb_array_schema_set_offsets_filter_list(
      ctx_->ptr().get(), schema_.get(), filters->ptr().get()));
}

void SchemaBuilder::set_validity_filter_list(
    std::shared_ptr<FilterList> filters) const {
  ctx_->handle_error(tiledb_array_schema_set_validity_filter_list(
      ctx_->ptr().get(), schema_.get(), filters->ptr().get()));
}

std::shared_ptr<SchemaBuilder> create_schema_builder(
    std::shared_ptr<Context> ctx, ArrayType atype) {
  return std::make_shared<SchemaBuilder>(ctx, atype);
}

}  // namespace tiledb::rs
