#ifndef TILEDB_RS_API_SUBARRAY_H
#define TILEDB_RS_API_SUBARRAY_H

#include <memory>
#include <string>
#include <vector>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Array;
class Config;
class Context;

class Subarray {
 public:
  Subarray(const Context& ctx, const Array& array, bool coalesce_ranges = true);

  uint64_t range_num(unsigned dim_idx) const;
  uint64_t range_num(const std::string& dim_name) const;

  template <class T>
  std::array<T, 3> range(unsigned dim_idx, uint64_t range_idx);

  template <class T>
  std::array<T, 3> range(const std::string& dim_name, uint64_t range_idx);

  std::array<std::string, 2> range(unsigned dim_idx, uint64_t range_idx);

  std::array<std::string, 2> range(
      const std::string& dim_name, uint64_t range_idx);

  std::shared_ptr<tiledb_subarray_t> ptr() const;
  const Array& array() const;

  uint64_t label_range_num(const std::string& label_name);

  template <class T>
  static std::array<T, 3> label_range(
      const std::string& label_name, uint64_t range_idx);

  std::array<std::string, 2> label_range(
      const std::string& label_name, uint64_t range_idx);

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<Array> array_;
  std::shared_ptr<tiledb_subarray_t> subarray_;
};

class SubarrayBuilder {
 public:
  void set_coalesce_ranges(bool coalesce_ranges);

  template <class T>
  void add_range(uint32_t dim_idx, T start, T end, T stride = 0);

  template <class T>
  void add_range(const std::string& dim_name, T start, T end, T stride = 0);

  void add_range(
      uint32_t dim_idx, const std::string& start, const std::string& end);

  void add_range(
      const std::string& dim_name,
      const std::string& start,
      const std::string& end);

  template <typename T = uint64_t>
  void set_subarray(const T* pairs, uint64_t size);

  void set_config(const Config& config);

  template <typename Vec>
  void set_subarray(const Vec& pairs);

  template <typename T = uint64_t>
  void set_subarray(const std::initializer_list<T>& l);

  template <typename T = uint64_t>
  void set_subarray(const std::vector<std::array<T, 2>>& pairs);

  template <class T>
  static void add_label_range(
      const std::string& label_name, T start, T end, T stride = 0);

  void add_label_range(
      const std::string& label_name,
      const std::string& start,
      const std::string& end);
};

}  // namespace tiledb::rs

#endif
