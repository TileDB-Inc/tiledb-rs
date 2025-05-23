#include <string>

#include <tiledb/tiledb.h>

#include "encryption_type.h"
#include "exception.h"

namespace tiledb::rs {

tiledb_encryption_type_t to_cpp_encryption_type(EncryptionType etype) {
  switch (etype) {
    case EncryptionType::None:
      return TILEDB_NO_ENCRYPTION;
    case EncryptionType::Aes256Gcm:
      return TILEDB_AES_256_GCM;
    default:
      throw TileDBError("Invalid TileOrder for conversion.");
  }
}

EncryptionType to_rs_emcryption_type(tiledb_encryption_type_t etype) {
  switch (etype) {
    case TILEDB_NO_ENCRYPTION:
      return EncryptionType::None;
    case TILEDB_AES_256_GCM:
      return EncryptionType::Aes256Gcm;
    default:
      throw TileDBError("Invalid tiledb_layout_t for TileOrder conversion.");
  }
}

}  // namespace tiledb::rs
