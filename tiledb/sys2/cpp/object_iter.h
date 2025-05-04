#ifndef TILEDB_RS_API_OBJECT_ITER_H
#define TILEDB_RS_API_OBJECT_ITER_H

#include <vector>

#include <tiledb/tiledb.h>

namespace tiledb::rs {

class Context;
class Object;

class ObjectIter {
 public:
  struct ObjGetterData {
    ObjGetterData(std::vector<Object>& objs, bool array, bool group);
    std::reference_wrapper<std::vector<Object>> objs_;
    bool array_;
    bool group_;
  };

  ObjectIter(Context& ctx, const std::string& root = ".");

  void set_iter_policy(bool group, bool array);

  void set_recursive(tiledb_walk_order_t walk_order = TILEDB_PREORDER);
  void set_non_recursive();

  class iterator {
   public:
    iterator(std::vector<Object> objs);

    iterator end();

   private:
    size_t cur_obj_;
    std::vector<Object> objs_;
  };

  iterator begin();

  iterator end() const;

  static int obj_getter(const char* path, tiledb_object_t type, void* data);

 private:
  std::shared_ptr<const Context> ctx_;

  bool array_;
  bool group_;
  bool recursive_;

  std::string root_;

  tiledb_walk_order_t walk_order_;
};

}  // namespace tiledb::rs

#endif
