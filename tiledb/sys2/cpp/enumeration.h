#ifndef TILEDB_RS_API_ENUMERATION_H
#define TILEDB_RS_API_ENUMERATION_H

#include <string>
#include <vector>

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

namespace tiledb::rs {

class Context;
class Enumeration;

std::shared_ptr<Enumeration> create_enumeration(
    const Context& ctx,
    const std::string& name,
    tiledb_datatype_t type,
    uint32_t cell_val_num,
    bool ordered,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size);

class Enumeration {
 public:
  Enumeration(const Context& ctx, tiledb_enumeration_t* enmr);

  std::shared_ptr<tiledb_enumeration_t> ptr() const;

  std::string name() const;
  tiledb_datatype_t type() const;
  uint32_t cell_val_num() const;
  bool ordered() const;

  template <typename T>
  std::vector<T> as_vector();

  template <typename T>
  std::optional<uint64_t> index_of(T value);

  Enumeration extend(
      const void* data,
      uint64_t data_size,
      const void* offsets,
      uint64_t offsets_size) const;

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_enumeration_t> enumeration_;
};

}  // namespace tiledb::rs

#endif
