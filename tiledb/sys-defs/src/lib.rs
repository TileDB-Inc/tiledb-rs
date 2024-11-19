#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

pub const tiledb_array_type_t_TILEDB_DENSE: tiledb_array_type_t = 0;
pub const tiledb_array_type_t_TILEDB_SPARSE: tiledb_array_type_t = 1;
pub type tiledb_array_type_t = ::std::os::raw::c_uint;

pub const tiledb_datatype_t_TILEDB_INT32: tiledb_datatype_t = 0;
pub const tiledb_datatype_t_TILEDB_INT64: tiledb_datatype_t = 1;
pub const tiledb_datatype_t_TILEDB_FLOAT32: tiledb_datatype_t = 2;
pub const tiledb_datatype_t_TILEDB_FLOAT64: tiledb_datatype_t = 3;
pub const tiledb_datatype_t_TILEDB_CHAR: tiledb_datatype_t = 4;
pub const tiledb_datatype_t_TILEDB_INT8: tiledb_datatype_t = 5;
pub const tiledb_datatype_t_TILEDB_UINT8: tiledb_datatype_t = 6;
pub const tiledb_datatype_t_TILEDB_INT16: tiledb_datatype_t = 7;
pub const tiledb_datatype_t_TILEDB_UINT16: tiledb_datatype_t = 8;
pub const tiledb_datatype_t_TILEDB_UINT32: tiledb_datatype_t = 9;
pub const tiledb_datatype_t_TILEDB_UINT64: tiledb_datatype_t = 10;
pub const tiledb_datatype_t_TILEDB_STRING_ASCII: tiledb_datatype_t = 11;
pub const tiledb_datatype_t_TILEDB_STRING_UTF8: tiledb_datatype_t = 12;
pub const tiledb_datatype_t_TILEDB_STRING_UTF16: tiledb_datatype_t = 13;
pub const tiledb_datatype_t_TILEDB_STRING_UTF32: tiledb_datatype_t = 14;
pub const tiledb_datatype_t_TILEDB_STRING_UCS2: tiledb_datatype_t = 15;
pub const tiledb_datatype_t_TILEDB_STRING_UCS4: tiledb_datatype_t = 16;
pub const tiledb_datatype_t_TILEDB_ANY: tiledb_datatype_t = 17;
pub const tiledb_datatype_t_TILEDB_DATETIME_YEAR: tiledb_datatype_t = 18;
pub const tiledb_datatype_t_TILEDB_DATETIME_MONTH: tiledb_datatype_t = 19;
pub const tiledb_datatype_t_TILEDB_DATETIME_WEEK: tiledb_datatype_t = 20;
pub const tiledb_datatype_t_TILEDB_DATETIME_DAY: tiledb_datatype_t = 21;
pub const tiledb_datatype_t_TILEDB_DATETIME_HR: tiledb_datatype_t = 22;
pub const tiledb_datatype_t_TILEDB_DATETIME_MIN: tiledb_datatype_t = 23;
pub const tiledb_datatype_t_TILEDB_DATETIME_SEC: tiledb_datatype_t = 24;
pub const tiledb_datatype_t_TILEDB_DATETIME_MS: tiledb_datatype_t = 25;
pub const tiledb_datatype_t_TILEDB_DATETIME_US: tiledb_datatype_t = 26;
pub const tiledb_datatype_t_TILEDB_DATETIME_NS: tiledb_datatype_t = 27;
pub const tiledb_datatype_t_TILEDB_DATETIME_PS: tiledb_datatype_t = 28;
pub const tiledb_datatype_t_TILEDB_DATETIME_FS: tiledb_datatype_t = 29;
pub const tiledb_datatype_t_TILEDB_DATETIME_AS: tiledb_datatype_t = 30;
pub const tiledb_datatype_t_TILEDB_TIME_HR: tiledb_datatype_t = 31;
pub const tiledb_datatype_t_TILEDB_TIME_MIN: tiledb_datatype_t = 32;
pub const tiledb_datatype_t_TILEDB_TIME_SEC: tiledb_datatype_t = 33;
pub const tiledb_datatype_t_TILEDB_TIME_MS: tiledb_datatype_t = 34;
pub const tiledb_datatype_t_TILEDB_TIME_US: tiledb_datatype_t = 35;
pub const tiledb_datatype_t_TILEDB_TIME_NS: tiledb_datatype_t = 36;
pub const tiledb_datatype_t_TILEDB_TIME_PS: tiledb_datatype_t = 37;
pub const tiledb_datatype_t_TILEDB_TIME_FS: tiledb_datatype_t = 38;
pub const tiledb_datatype_t_TILEDB_TIME_AS: tiledb_datatype_t = 39;
pub const tiledb_datatype_t_TILEDB_BLOB: tiledb_datatype_t = 40;
pub const tiledb_datatype_t_TILEDB_BOOL: tiledb_datatype_t = 41;
pub const tiledb_datatype_t_TILEDB_GEOM_WKB: tiledb_datatype_t = 42;
pub const tiledb_datatype_t_TILEDB_GEOM_WKT: tiledb_datatype_t = 43;
pub type tiledb_datatype_t = ::std::os::raw::c_uint;

pub const tiledb_encryption_type_t_TILEDB_NO_ENCRYPTION:
    tiledb_encryption_type_t = 0;
pub const tiledb_encryption_type_t_TILEDB_AES_256_GCM:
    tiledb_encryption_type_t = 1;
pub type tiledb_encryption_type_t = ::std::os::raw::c_uint;

pub const tiledb_filesystem_t_TILEDB_HDFS: tiledb_filesystem_t = 0;
pub const tiledb_filesystem_t_TILEDB_S3: tiledb_filesystem_t = 1;
pub const tiledb_filesystem_t_TILEDB_AZURE: tiledb_filesystem_t = 2;
pub const tiledb_filesystem_t_TILEDB_GCS: tiledb_filesystem_t = 3;
pub const tiledb_filesystem_t_TILEDB_MEMFS: tiledb_filesystem_t = 4;
pub type tiledb_filesystem_t = ::std::os::raw::c_uint;

pub const tiledb_filter_type_t_TILEDB_FILTER_NONE: tiledb_filter_type_t = 0;
pub const tiledb_filter_type_t_TILEDB_FILTER_GZIP: tiledb_filter_type_t = 1;
pub const tiledb_filter_type_t_TILEDB_FILTER_ZSTD: tiledb_filter_type_t = 2;
pub const tiledb_filter_type_t_TILEDB_FILTER_LZ4: tiledb_filter_type_t = 3;
pub const tiledb_filter_type_t_TILEDB_FILTER_RLE: tiledb_filter_type_t = 4;
pub const tiledb_filter_type_t_TILEDB_FILTER_BZIP2: tiledb_filter_type_t = 5;
pub const tiledb_filter_type_t_TILEDB_FILTER_DOUBLE_DELTA:
    tiledb_filter_type_t = 6;
pub const tiledb_filter_type_t_TILEDB_FILTER_BIT_WIDTH_REDUCTION:
    tiledb_filter_type_t = 7;
pub const tiledb_filter_type_t_TILEDB_FILTER_BITSHUFFLE: tiledb_filter_type_t =
    8;
pub const tiledb_filter_type_t_TILEDB_FILTER_BYTESHUFFLE: tiledb_filter_type_t =
    9;
pub const tiledb_filter_type_t_TILEDB_FILTER_POSITIVE_DELTA:
    tiledb_filter_type_t = 10;
pub const tiledb_filter_type_t_TILEDB_FILTER_CHECKSUM_MD5:
    tiledb_filter_type_t = 12;
pub const tiledb_filter_type_t_TILEDB_FILTER_CHECKSUM_SHA256:
    tiledb_filter_type_t = 13;
pub const tiledb_filter_type_t_TILEDB_FILTER_DICTIONARY: tiledb_filter_type_t =
    14;
pub const tiledb_filter_type_t_TILEDB_FILTER_SCALE_FLOAT: tiledb_filter_type_t =
    15;
pub const tiledb_filter_type_t_TILEDB_FILTER_XOR: tiledb_filter_type_t = 16;
pub const tiledb_filter_type_t_TILEDB_FILTER_DEPRECATED: tiledb_filter_type_t =
    17;
pub const tiledb_filter_type_t_TILEDB_FILTER_WEBP: tiledb_filter_type_t = 18;
pub const tiledb_filter_type_t_TILEDB_FILTER_DELTA: tiledb_filter_type_t = 19;
pub type tiledb_filter_type_t = ::std::os::raw::c_uint;

pub const tiledb_filter_option_t_TILEDB_COMPRESSION_LEVEL:
    tiledb_filter_option_t = 0;
pub const tiledb_filter_option_t_TILEDB_BIT_WIDTH_MAX_WINDOW:
    tiledb_filter_option_t = 1;
pub const tiledb_filter_option_t_TILEDB_POSITIVE_DELTA_MAX_WINDOW:
    tiledb_filter_option_t = 2;
pub const tiledb_filter_option_t_TILEDB_SCALE_FLOAT_BYTEWIDTH:
    tiledb_filter_option_t = 3;
pub const tiledb_filter_option_t_TILEDB_SCALE_FLOAT_FACTOR:
    tiledb_filter_option_t = 4;
pub const tiledb_filter_option_t_TILEDB_SCALE_FLOAT_OFFSET:
    tiledb_filter_option_t = 5;
pub const tiledb_filter_option_t_TILEDB_WEBP_QUALITY: tiledb_filter_option_t =
    6;
pub const tiledb_filter_option_t_TILEDB_WEBP_INPUT_FORMAT:
    tiledb_filter_option_t = 7;
pub const tiledb_filter_option_t_TILEDB_WEBP_LOSSLESS: tiledb_filter_option_t =
    8;
pub const tiledb_filter_option_t_TILEDB_COMPRESSION_REINTERPRET_DATATYPE:
    tiledb_filter_option_t = 9;
pub type tiledb_filter_option_t = ::std::os::raw::c_uint;

// N.B. bindgen does not generate this enumeration. I believe that's because
// its never used as a type for a function argument.
pub const tiledb_filter_webp_format_t_TILEDB_WEBP_NONE:
    tiledb_filter_webp_format_t = 0;
pub const tiledb_filter_webp_format_t_TILEDB_WEBP_RGB:
    tiledb_filter_webp_format_t = 1;
pub const tiledb_filter_webp_format_t_TILEDB_WEBP_BGR:
    tiledb_filter_webp_format_t = 2;
pub const tiledb_filter_webp_format_t_TILEDB_WEBP_RGBA:
    tiledb_filter_webp_format_t = 3;
pub const tiledb_filter_webp_format_t_TILEDB_WEBP_BGRA:
    tiledb_filter_webp_format_t = 4;
pub type tiledb_filter_webp_format_t = ::std::os::raw::c_uint;

pub const tiledb_layout_t_TILEDB_ROW_MAJOR: tiledb_layout_t = 0;
pub const tiledb_layout_t_TILEDB_COL_MAJOR: tiledb_layout_t = 1;
pub const tiledb_layout_t_TILEDB_GLOBAL_ORDER: tiledb_layout_t = 2;
pub const tiledb_layout_t_TILEDB_UNORDERED: tiledb_layout_t = 3;
pub const tiledb_layout_t_TILEDB_HILBERT: tiledb_layout_t = 4;
pub type tiledb_layout_t = ::std::os::raw::c_uint;

pub const tiledb_object_t_TILEDB_INVALID: tiledb_object_t = 0;
pub const tiledb_object_t_TILEDB_GROUP: tiledb_object_t = 1;
pub const tiledb_object_t_TILEDB_ARRAY: tiledb_object_t = 2;
pub type tiledb_object_t = ::std::os::raw::c_uint;

pub const tiledb_query_condition_combination_op_t_TILEDB_AND:
    tiledb_query_condition_combination_op_t = 0;
pub const tiledb_query_condition_combination_op_t_TILEDB_OR:
    tiledb_query_condition_combination_op_t = 1;
pub const tiledb_query_condition_combination_op_t_TILEDB_NOT:
    tiledb_query_condition_combination_op_t = 2;
pub type tiledb_query_condition_combination_op_t = ::std::os::raw::c_uint;

pub const tiledb_query_condition_op_t_TILEDB_LT: tiledb_query_condition_op_t =
    0;
pub const tiledb_query_condition_op_t_TILEDB_LE: tiledb_query_condition_op_t =
    1;
pub const tiledb_query_condition_op_t_TILEDB_GT: tiledb_query_condition_op_t =
    2;
pub const tiledb_query_condition_op_t_TILEDB_GE: tiledb_query_condition_op_t =
    3;
pub const tiledb_query_condition_op_t_TILEDB_EQ: tiledb_query_condition_op_t =
    4;
pub const tiledb_query_condition_op_t_TILEDB_NE: tiledb_query_condition_op_t =
    5;
pub const tiledb_query_condition_op_t_TILEDB_IN: tiledb_query_condition_op_t =
    6;
pub const tiledb_query_condition_op_t_TILEDB_NOT_IN:
    tiledb_query_condition_op_t = 7;
pub type tiledb_query_condition_op_t = ::std::os::raw::c_uint;

pub const tiledb_query_status_t_TILEDB_FAILED: tiledb_query_status_t = 0;
pub const tiledb_query_status_t_TILEDB_COMPLETED: tiledb_query_status_t = 1;
pub const tiledb_query_status_t_TILEDB_INPROGRESS: tiledb_query_status_t = 2;
pub const tiledb_query_status_t_TILEDB_INCOMPLETE: tiledb_query_status_t = 3;
pub const tiledb_query_status_t_TILEDB_UNINITIALIZED: tiledb_query_status_t = 4;
pub const tiledb_query_status_t_TILEDB_INITIALIZED: tiledb_query_status_t = 5;
pub type tiledb_query_status_t = ::std::os::raw::c_uint;

pub const tiledb_query_type_t_TILEDB_READ: tiledb_query_type_t = 0;
pub const tiledb_query_type_t_TILEDB_WRITE: tiledb_query_type_t = 1;
pub const tiledb_query_type_t_TILEDB_DELETE: tiledb_query_type_t = 2;
pub const tiledb_query_type_t_TILEDB_UPDATE: tiledb_query_type_t = 3;
pub const tiledb_query_type_t_TILEDB_MODIFY_EXCLUSIVE: tiledb_query_type_t = 4;
pub type tiledb_query_type_t = ::std::os::raw::c_uint;

pub const tiledb_field_origin_t_TILEDB_ATTRIBUTE_FIELD: tiledb_field_origin_t =
    0;
pub const tiledb_field_origin_t_TILEDB_DIMENSION_FIELD: tiledb_field_origin_t =
    1;
pub const tiledb_field_origin_t_TILEDB_AGGREGATE_FIELD: tiledb_field_origin_t =
    2;
pub type tiledb_field_origin_t = ::std::os::raw::c_uint;

pub const tiledb_vfs_mode_t_TILEDB_VFS_READ: tiledb_vfs_mode_t = 0;
pub const tiledb_vfs_mode_t_TILEDB_VFS_WRITE: tiledb_vfs_mode_t = 1;
pub const tiledb_vfs_mode_t_TILEDB_VFS_APPEND: tiledb_vfs_mode_t = 2;
pub type tiledb_vfs_mode_t = ::std::os::raw::c_uint;

pub const tiledb_walk_order_t_TILEDB_PREORDER: tiledb_walk_order_t = 0;
pub const tiledb_walk_order_t_TILEDB_POSTORDER: tiledb_walk_order_t = 1;
pub type tiledb_walk_order_t = ::std::os::raw::c_uint;
