#ifndef TILEDB_RS_API_CONSOLIDATION_PLAN_H
#define TILEDB_RS_API_CONSOLIDATION_PLAN_H

#include <string>

#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>

namespace tiledb::rs {

class Array;
class Context;

class ConsolidationPlan {
 public:
  ConsolidationPlan(
      const Context& ctx, const Array& array, uint64_t fragment_size);

  uint64_t num_nodes() const;
  uint64_t num_fragments(uint64_t node_idx) const;

  std::string fragment_uri(uint64_t node_idx, uint64_t fragment_idx) const;

  std::string to_json() const;

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_consolidation_plan_t> consolidation_plan_;
};

}  // namespace tiledb::rs

#endif
