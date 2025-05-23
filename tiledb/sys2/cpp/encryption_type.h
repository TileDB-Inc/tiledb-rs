#ifndef TILEDB_RS_API_ENCRYPTION_TYPE_H
#define TILEDB_RS_API_ENCRYPTION_TYPE_H

#include <tiledb/tiledb.h>

#include "tiledb-sys2/src/encryption_type.rs.h"

namespace tiledb::rs {

tiledb_encryption_type_t to_cpp_encryption_type(EncryptionType etype);
EncryptionType to_rs_encryption_type(tiledb_encryption_type_t etype);

}  // namespace tiledb::rs

#endif
