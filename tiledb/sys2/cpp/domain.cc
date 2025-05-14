#include <memory>
#include <string>

#include <tiledb/tiledb_experimental.h>

#include "context.h"
#include "datatype.h"
#include "dimension.h"
#include "domain.h"
#include "filter_list.h"

namespace tiledb::rs {

static void delete_domain(tiledb_domain_t* dom) {
  if (dom != nullptr) {
    tiledb_domain_free(&dom);
  }
}

Domain::Domain(std::shared_ptr<Context> ctx, tiledb_domain_t* dom)
    : ctx_(ctx)
    , dom_(dom, delete_domain) {
}

Domain::Domain(
    std::shared_ptr<Context> ctx, std::shared_ptr<tiledb_domain_t> dom)
    : ctx_(ctx)
    , dom_(dom) {
}

Datatype Domain::datatype() const {
  tiledb_datatype_t dtype;
  ctx_->handle_error(
      tiledb_domain_get_type(ctx_->ptr().get(), dom_.get(), &dtype));
  return to_rs_datatype(dtype);
}

uint32_t Domain::num_dimensions() const {
  uint32_t count;
  ctx_->handle_error(
      tiledb_domain_get_ndim(ctx_->ptr().get(), dom_.get(), &count));
  return count;
}

std::shared_ptr<Dimension> Domain::dimension_from_index(uint32_t idx) const {
  tiledb_dimension_t* dim = nullptr;
  ctx_->handle_error(tiledb_domain_get_dimension_from_index(
      ctx_->ptr().get(), dom_.get(), idx, &dim));
  return std::make_shared<Dimension>(ctx_, dim);
}

std::shared_ptr<Dimension> Domain::dimension_from_name(rust::Str name) const {
  auto c_name = static_cast<std::string>(name);
  tiledb_dimension_t* dim = nullptr;
  ctx_->handle_error(tiledb_domain_get_dimension_from_name(
      ctx_->ptr().get(), dom_.get(), c_name.c_str(), &dim));
  return std::make_shared<Dimension>(ctx_, dim);
}

bool Domain::has_dimension(rust::Str name) const {
  auto c_name = static_cast<std::string>(name);
  int32_t exists = 0;
  ctx_->handle_error(tiledb_domain_has_dimension(
      ctx_->ptr().get(), dom_.get(), c_name.c_str(), &exists));
  return exists != 0;
}

std::shared_ptr<tiledb_domain_t> Domain::ptr() const {
  return dom_;
}

DomainBuilder::DomainBuilder(std::shared_ptr<Context> ctx)
    : ctx_(ctx) {
  tiledb_domain_t* dom;
  ctx_->handle_error(tiledb_domain_alloc(ctx_->ptr().get(), &dom));
  dom_ = std::shared_ptr<tiledb_domain_t>(dom, delete_domain);
}

std::shared_ptr<Domain> DomainBuilder::build() const {
  return std::make_shared<Domain>(ctx_, dom_);
}

void DomainBuilder::add_dimension(std::shared_ptr<Dimension> dim) const {
  ctx_->handle_error(tiledb_domain_add_dimension(
      ctx_->ptr().get(), dom_.get(), dim->ptr().get()));
}

std::shared_ptr<DomainBuilder> create_domain_builder(
    std::shared_ptr<Context> ctx) {
  return std::make_shared<DomainBuilder>(ctx);
}

}  // namespace tiledb::rs
