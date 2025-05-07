#ifndef TILEDB_RS_API_FILTER_LIST_H
#define TILEDB_RS_API_FILTER_LIST_H

#include <memory>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Context;
class Filter;

class FilterList {
 public:
  FilterList(std::shared_ptr<Context> ctx, tiledb_filter_list_t* filter_list);
  FilterList(
      std::shared_ptr<Context> ctx,
      std::shared_ptr<tiledb_filter_list_t> filter_list);

  std::shared_ptr<Filter> get_filter(uint32_t index) const;
  uint32_t num_filters() const;
  uint32_t max_chunk_size() const;

  std::shared_ptr<tiledb_filter_list_t> ptr() const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_filter_list_t> filter_list_;
};

class FilterListBuilder {
 public:
  FilterListBuilder(std::shared_ptr<Context> ctx);

  std::shared_ptr<FilterList> build() const;

  void add_filter(const std::shared_ptr<Filter> filter) const;
  void set_max_chunk_size(uint32_t max_chunk_size) const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_filter_list_t> filter_list_;
};

std::shared_ptr<FilterListBuilder> create_filter_list_builder(
    std::shared_ptr<Context> ctx);

}  // namespace tiledb::rs

#endif
