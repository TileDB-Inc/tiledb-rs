#ifndef TILEDB_RS_API_FRAGMENT_INFO_H
#define TILEDB_RS_API_FRAGMENT_INFO_H

#include <string>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Context;
class Schema;

class FragmentInfo {
 public:
  FragmentInfo(const Context& ctx, const std::string& array_uri);

  void load() const;
  std::string fragment_uri(uint32_t fid) const;
  std::string fragment_name(uint32_t fid) const;

  void get_non_empty_domain(uint32_t fid, uint32_t did, void* domain) const;

  void get_non_empty_domain(
      uint32_t fid, const std::string& dim_name, void* domain) const;

  std::pair<std::string, std::string> non_empty_domain_var(
      uint32_t fid, uint32_t did) const;

  std::pair<std::string, std::string> non_empty_domain_var(
      uint32_t fid, const std::string& dim_name) const;

  uint64_t mbr_num(uint32_t fid) const;

  void get_mbr(uint32_t fid, uint32_t mid, uint32_t did, void* mbr) const;

  void get_mbr(
      uint32_t fid, uint32_t mid, const std::string& dim_name, void* mbr) const;

  std::pair<std::string, std::string> mbr_var(
      uint32_t fid, uint32_t mid, uint32_t did) const;

  std::pair<std::string, std::string> mbr_var(
      uint32_t fid, uint32_t mid, const std::string& dim_name) const;

  uint32_t fragment_num() const;

  uint64_t fragment_size(uint32_t fid) const;

  bool dense(uint32_t fid) const;

  bool sparse(uint32_t fid) const;

  std::pair<uint64_t, uint64_t> timestamp_range(uint32_t fid) const;

  uint64_t cell_num(uint32_t fid) const;

  uint64_t total_cell_num() const;

  uint32_t version(uint32_t fid) const;

  Schema array_schema(uint32_t fid) const;

  std::string array_schema_name(uint32_t fid) const;

  bool has_consolidated_metadata(uint32_t fid) const;

  uint32_t unconsolidated_metadata_num() const;

  uint32_t to_vacuum_num() const;

  std::string to_vacuum_uri(uint32_t fid) const;

  std::shared_ptr<tiledb_fragment_info_t> ptr() const;

 private:
  std::shared_ptr<const Context> ctx_;
  std::shared_ptr<tiledb_fragment_info_t> fragment_info_;
};

}  // namespace tiledb::rs

#endif
