#ifndef TILEDB_RS_API_NDRECTANGLE_H
#define TILEDB_RS_API_NDRECTANGLE_H

#include <string>
#include <type_traits>

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

namespace tiledb::rs {

class Context;
class Domain;

class NDRectangle {
 public:
  NDRectangle(const Context& ctx, const Domain& domain);
  NDRectangle(const Context& ctx, tiledb_ndrectangle_t* ndrect);

  template <
      class T,
      std::enable_if_t<
          std::is_integral_v<T> || std::is_floating_point_v<T>,
          bool> = true>
  NDRectangle& set_range(const std::string& dim_name, T start, T end);

  template <
      class T,
      std::enable_if_t<
          std::is_integral_v<T> || std::is_floating_point_v<T>,
          bool> = true>
  NDRectangle& set_range(uint32_t dim_idx, T start, T end);

  NDRectangle& set_range(
      uint32_t dim_idx, const std::string& start, const std::string& end);

  NDRectangle& set_range(
      const std::string& dim_name,
      const std::string& start,
      const std::string& end);

  template <class T>
  std::array<T, 2> range(const std::string& dim_name);

  template <class T>
  std::array<T, 2> range(unsigned dim_idx);

  std::shared_ptr<tiledb_ndrectangle_t> ptr() const;

  tiledb_datatype_t range_dtype(unsigned dim_idx);

  tiledb_datatype_t range_dtype(const std::string& dim_name);

  uint32_t dim_num();

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_ndrectangle_t> ndrect_;
};

}  // namespace tiledb::rs

#endif
