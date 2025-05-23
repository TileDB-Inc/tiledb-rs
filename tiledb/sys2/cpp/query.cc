#include <memory>
#include <string>

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

#include "array.h"
#include "context.h"
#include "dimension.h"
#include "domain.h"
#include "enumeration.h"
#include "layout.h"
#include "mode.h"
#include "query.h"
#include "query_status.h"
#include "schema.h"

namespace tiledb::rs {

static void delete_query(tiledb_query_t* query) {
  if (query != nullptr) {
    tiledb_query_free(&query);
  }
}

Query::Query(
    std::shared_ptr<Context> ctx,
    std::shared_ptr<Array> array,
    tiledb_query_t* query)
    : ctx_(ctx)
    , array_(array)
    , query_(query, delete_query) {
}

Query::Query(
    std::shared_ptr<Context> ctx,
    std::shared_ptr<Array> array,
    std::shared_ptr<tiledb_query_t> query)
    : ctx_(ctx)
    , array_(array)
    , query_(query) {
}

Mode Query::mode() const {
  tiledb_query_type_t mode;
  ctx_->handle_error(
      tiledb_query_get_type(ctx_->ptr().get(), query_.get(), &mode));
  return to_rs_mode(mode);
}

std::shared_ptr<Config> Query::config() const {
  tiledb_config_t* cfg = nullptr;
  ctx_->handle_error(
      tiledb_query_get_config(ctx_->ptr().get(), query_.get(), &cfg));
  return std::make_shared<Config>(cfg);
}

CellOrder Query::layout() const {
  tiledb_layout_t order;
  ctx_->handle_error(
      tiledb_query_get_layout(ctx_->ptr().get(), query_.get(), &order));
  return to_rs_cell_order(order);
}

QueryStatus Query::status() const {
  tiledb_query_status_t status;
  ctx_->handle_error(
      tiledb_query_get_status(ctx_->ptr().get(), query_.get(), &status));
  return to_rs_query_status(status);
}

bool Query::has_results() const {
  int res;
  ctx_->handle_error(
      tiledb_query_has_results(ctx_->ptr().get(), query_.get(), &res));
  return res != 0;
}

void Query::set_data_buffer(rust::Str name, Buffer& data) const {
  auto c_name = static_cast<std::string>(name);
  auto sizes = (*sizes_)[c_name];

  sizes->data = data.len();

  ctx_->handle_error(tiledb_query_set_data_buffer(
      ctx_->ptr().get(),
      query_.get(),
      c_name.c_str(),
      data.as_mut_ptr(),
      &(sizes->data)));
}

void Query::set_offsets_buffer(rust::Str name, Buffer& offsets) const {
  auto c_name = static_cast<std::string>(name);
  auto sizes = (*sizes_)[c_name];

  sizes->offsets = offsets.len();

  ctx_->handle_error(tiledb_query_set_data_buffer(
      ctx_->ptr().get(),
      query_.get(),
      c_name.c_str(),
      offsets.as_mut_ptr(),
      &(sizes->offsets)));
}

void Query::set_validity_buffer(rust::Str name, Buffer& validity) const {
  auto c_name = static_cast<std::string>(name);
  auto sizes = (*sizes_)[c_name];

  sizes->validity = validity.len();

  ctx_->handle_error(tiledb_query_set_data_buffer(
      ctx_->ptr().get(),
      query_.get(),
      c_name.c_str(),
      validity.as_mut_ptr(),
      &(sizes->validity)));
}

void Query::submit() const {
  ctx_->handle_error(tiledb_query_submit(ctx_->ptr().get(), query_.get()));
}

void Query::finalize() const {
  ctx_->handle_error(tiledb_query_finalize(ctx_->ptr().get(), query_.get()));
}

void Query::submit_and_finalize() const {
  ctx_->handle_error(
      tiledb_query_submit_and_finalize(ctx_->ptr().get(), query_.get()));
}

void Query::est_result_size(
    rust::Str name,
    uint64_t& data,
    uint64_t& offsets,
    uint64_t& validity) const {
  auto c_name = static_cast<std::string>(name);
  ctx_->handle_error(tiledb_query_get_est_result_size_var_nullable(
      ctx_->ptr().get(),
      query_.get(),
      c_name.c_str(),
      &data,
      &offsets,
      &validity));
}

uint32_t Query::num_fragments() const {
  uint32_t num;
  ctx_->handle_error(
      tiledb_query_get_fragment_num(ctx_->ptr().get(), query_.get(), &num));
  return num;
}

uint64_t Query::num_relevant_fragments() const {
  uint64_t num;
  ctx_->handle_error(tiledb_query_get_relevant_fragment_num(
      ctx_->ptr().get(), query_.get(), &num));
  return num;
}

rust::String Query::fragment_uri(uint32_t index) const {
  const char* c_uri;
  ctx_->handle_error(tiledb_query_get_fragment_uri(
      ctx_->ptr().get(), query_.get(), index, &c_uri));
  return c_uri;
}

void Query::fragment_timestamp_range(
    uint32_t index, uint64_t& lo, uint64_t& hi) const {
  ctx_->handle_error(tiledb_query_get_fragment_timestamp_range(
      ctx_->ptr().get(), query_.get(), index, &lo, &hi));
}

rust::String Query::stats() const {
  char* stats;
  ctx_->handle_error(
      tiledb_query_get_stats(ctx_->ptr().get(), query_.get(), &stats));
  return stats;
}

std::shared_ptr<tiledb_query_t> Query::ptr() const {
  return query_;
}

QueryBuilder::QueryBuilder(
    std::shared_ptr<Context> ctx, std::shared_ptr<Array> array, Mode mode)
    : ctx_(ctx)
    , array_(array) {
  auto c_mode = to_cpp_mode(mode);
  tiledb_query_t* query = nullptr;
  ctx_->handle_error(tiledb_query_alloc(
      ctx_->ptr().get(), array_->ptr().get(), c_mode, &query));
  query_ = std::shared_ptr<tiledb_query_t>(query, delete_query);
}

void QueryBuilder::set_layout(CellOrder order) const {
  auto c_order = to_cpp_cell_order(order);
  ctx_->handle_error(
      tiledb_query_set_layout(ctx_->ptr().get(), query_.get(), c_order));
}

void QueryBuilder::set_config(std::shared_ptr<Config> cfg) const {
  ctx_->handle_error(tiledb_query_set_config(
      ctx_->ptr().get(), query_.get(), cfg->ptr().get()));
}

std::shared_ptr<QueryBuilder> create_query_builder(
    std::shared_ptr<Context> ctx, std::shared_ptr<Array> array, Mode mode) {
  return std::make_shared<QueryBuilder>(ctx, array, mode);
}

}  // namespace tiledb::rs
