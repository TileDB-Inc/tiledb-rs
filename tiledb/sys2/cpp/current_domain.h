#ifndef TILEDB_RS_API_CURRENT_DOMAIN_H
#define TILEDB_RS_API_CURRENT_DOMAIN_H

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

#include <memory>

namespace tiledb::rs {

class Context;
class NDRectangle;

class CurrentDomain {
 public:
  CurrentDomain(const Context& ctx);
  CurrentDomain(const Context& ctx, tiledb_current_domain_t* cd);

  std::shared_ptr<tiledb_current_domain_t> ptr() const;

  tiledb_current_domain_type_t type() const;

  CurrentDomain& set_ndrectangle(const NDRectangle& ndrect);

  NDRectangle ndrectangle() const;
  bool is_empty() const;

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_current_domain_t> current_domain_;
};

}  // namespace tiledb::rs

#endif
