#ifndef TILEDB_RS_API_DIMENSION_LABEL_H
#define TILEDB_RS_API_DIMENSION_LABEL_H

#include <string>

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

namespace tiledb::rs {

class Context;

class DimensionLabel {
 public:
  DimensionLabel(const Context& ctx, tiledb_dimension_label_t* dim_label);

  uint32_t dimension_index() const;
  std::string label_attr_name() const;
  uint32_t label_cell_val_num() const;
  tiledb_data_order_t label_order() const;
  tiledb_datatype_t label_type() const;

  std::string name() const;

  std::shared_ptr<tiledb_dimension_label_t> ptr() const;

  std::string uri() const;

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_dimension_label_t> dim_label_;
};

}  // namespace tiledb::rs

#endif
