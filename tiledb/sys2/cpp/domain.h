#ifndef TILEDB_RS_API_DOMAIN_H
#define TILEDB_RS_API_DOMAIN_H

#include <tiledb/tiledb.h>

#include "rust/cxx.h"
#include "tiledb-sys2/src/datatype.rs.h"

namespace tiledb::rs {

class Context;
class Dimension;

class Domain {
 public:
  Domain(std::shared_ptr<Context> ctx, tiledb_domain_t* domain);
  Domain(std::shared_ptr<Context> ctx, std::shared_ptr<tiledb_domain_t> domain);

  Datatype datatype() const;
  uint32_t num_dimensions() const;
  std::shared_ptr<Dimension> dimension_from_index(uint32_t idx) const;
  std::shared_ptr<Dimension> dimension_from_name(rust::Str name) const;
  bool has_dimension(rust::Str name) const;

  std::shared_ptr<tiledb_domain_t> ptr() const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_domain_t> dom_;
};

class DomainBuilder {
 public:
  DomainBuilder(std::shared_ptr<Context> ctx);

  std::shared_ptr<Domain> build() const;

  void add_dimension(std::shared_ptr<Dimension> dim) const;

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<tiledb_domain_t> dom_;
};

std::shared_ptr<DomainBuilder> create_domain_builder(
    std::shared_ptr<Context> ctx);

}  // namespace tiledb::rs

#endif
