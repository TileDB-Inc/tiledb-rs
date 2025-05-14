#ifndef TILEDB_RS_API_ENUMERATION_H
#define TILEDB_RS_API_ENUMERATION_H

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

#include "rust/cxx.h"
#include "tiledb-sys2/src/buffer.rs.h"
#include "tiledb-sys2/src/datatype.rs.h"

namespace tiledb::rs {

class Context;
class Enumeration;

class Enumeration {
 public:
  Enumeration(std::shared_ptr<Context> ctx, tiledb_enumeration_t* enmr);

  rust::String name() const;
  Datatype datatype() const;
  uint32_t cell_val_num() const;
  bool ordered() const;

  void get_data(Buffer& buf) const;
  void get_offsets(Buffer& buf) const;
  bool get_index(Buffer& buf, uint64_t& index) const;

  std::shared_ptr<Enumeration> extend(Buffer& data, Buffer& offsets) const;

  std::shared_ptr<tiledb_enumeration_t> ptr() const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_enumeration_t> enmr_;
};

std::shared_ptr<Enumeration> create_enumeration(
    std::shared_ptr<Context> ctx,
    rust::Str name,
    Datatype type,
    uint32_t cell_val_num,
    bool ordered,
    Buffer& data,
    Buffer& offsets);

}  // namespace tiledb::rs

#endif
