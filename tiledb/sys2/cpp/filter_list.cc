
#include "filter_list.h"
#include "context.h"
#include "filter.h"

namespace tiledb::rs {

static void delete_filter_list(tiledb_filter_list_t* filter_list) {
  if (filter_list != nullptr) {
    tiledb_filter_list_free(&filter_list);
  }
}

FilterList::FilterList(
    std::shared_ptr<Context> ctx, tiledb_filter_list_t* filter_list)
    : ctx_(ctx)
    , filter_list_(filter_list, delete_filter_list) {
}

FilterList::FilterList(
    std::shared_ptr<Context> ctx,
    std::shared_ptr<tiledb_filter_list_t> filter_list)
    : ctx_(ctx)
    , filter_list_(filter_list) {
}

std::shared_ptr<Filter> FilterList::get_filter(uint32_t index) const {
  tiledb_filter_t* filter;
  ctx_->handle_error(tiledb_filter_list_get_filter_from_index(
      ctx_->ptr().get(), filter_list_.get(), index, &filter));
  return std::make_shared<Filter>(ctx_, filter);
}

uint32_t FilterList::num_filters() const {
  uint32_t num_filters;
  ctx_->handle_error(tiledb_filter_list_get_nfilters(
      ctx_->ptr().get(), filter_list_.get(), &num_filters));
  return num_filters;
}

uint32_t FilterList::max_chunk_size() const {
  uint32_t max_chunk_size;
  ctx_->handle_error(tiledb_filter_list_get_max_chunk_size(
      ctx_->ptr().get(), filter_list_.get(), &max_chunk_size));
  return max_chunk_size;
}

FilterListBuilder::FilterListBuilder(std::shared_ptr<Context> ctx)
    : ctx_(ctx) {
  tiledb_filter_list_t* filter_list;
  ctx_->handle_error(tiledb_filter_list_alloc(ctx_->ptr().get(), &filter_list));
  filter_list_ =
      std::shared_ptr<tiledb_filter_list_t>(filter_list, delete_filter_list);
}

std::shared_ptr<FilterList> FilterListBuilder::build() const {
  return std::make_shared<FilterList>(ctx_, filter_list_);
}

void FilterListBuilder::add_filter(const std::shared_ptr<Filter> filter) const {
  ctx_->handle_error(tiledb_filter_list_add_filter(
      ctx_->ptr().get(), filter_list_.get(), filter->ptr().get()));
}

void FilterListBuilder::set_max_chunk_size(uint32_t max_chunk_size) const {
  ctx_->handle_error(tiledb_filter_list_set_max_chunk_size(
      ctx_->ptr().get(), filter_list_.get(), max_chunk_size));
}

std::shared_ptr<FilterListBuilder> create_filter_list_builder(
    std::shared_ptr<Context> ctx) {
  return std::make_shared<FilterListBuilder>(ctx);
}

}  // namespace tiledb::rs
