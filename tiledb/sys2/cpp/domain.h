#ifndef TILEDB_RS_API_DOMAIN_H
#define TILEDB_RS_API_DOMAIN_H

#include <string>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Context;
class Dimension;

class Domain {
 public:
  Domain(std::shared_ptr<const Context> ctx);
  Domain(std::shared_ptr<const Context> ctx, tiledb_domain_t* domain);

  tiledb_datatype_t type() const;
  uint64_t cell_num() const;
  uint32_t num_dimensions() const;
  Dimension dimension(unsigned idx) const;
  Dimension dimension(const std::string& name) const;
  bool has_dimension(const std::string& name) const;

  std::shared_ptr<tiledb_domain_t> ptr() const;

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_domain_t> domain_;
};

class DomainBuilder {
 public:
  DomainBuilder(std::shared_ptr<const Context> ctx);

  void add_dimension(const Dimension& dim);

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_domain_t> domain_;
};

}  // namespace tiledb::rs

#endif
