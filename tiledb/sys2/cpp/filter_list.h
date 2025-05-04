#ifndef TILEDB_RS_API_FILTER_LIST_H
#define TILEDB_RS_API_FILTER_LIST_H

#include <memory>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Context;
class Filter;

class FilterList {
 public:
  FilterList(const Context& ctx, tiledb_filter_list_t* filter_list);

  Filter filter(uint32_t filter_index) const;
  uint32_t max_chunk_size() const;
  uint32_t num_filters() const;

  std::shared_ptr<tiledb_filter_list_t> ptr() const;

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_filter_list_t> filter_list_;
};

class FilterListBuilder {
 public:
  FilterListBuilder(std::shared_ptr<const Context> ctx);

  void add_filter(const Filter& filter);
  FilterList& set_max_chunk_size(uint32_t max_chunk_size);

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_filter_list_t> filter_list_;
};

}  // namespace tiledb::rs

#endif
