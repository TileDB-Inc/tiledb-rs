#include <string>

#include <tiledb/tiledb.h>

#include "exception.h"
#include "tiledb-sys2/src/datatype.rs.h"

namespace tiledb::rs {

tiledb_datatype_t to_cpp_datatype(Datatype dt) {
  switch (dt) {
    case Datatype::Int32:
      return TILEDB_INT32;
    case Datatype::Int64:
      return TILEDB_INT64;
    case Datatype::Float32:
      return TILEDB_FLOAT32;
    case Datatype::Float64:
      return TILEDB_FLOAT64;
    case Datatype::Char:
      return TILEDB_CHAR;
    case Datatype::Int8:
      return TILEDB_INT8;
    case Datatype::UInt8:
      return TILEDB_UINT8;
    case Datatype::Int16:
      return TILEDB_INT16;
    case Datatype::UInt16:
      return TILEDB_UINT16;
    case Datatype::UInt32:
      return TILEDB_UINT32;
    case Datatype::UInt64:
      return TILEDB_UINT64;
    case Datatype::StringAscii:
      return TILEDB_STRING_ASCII;
    case Datatype::StringUtf8:
      return TILEDB_STRING_UTF8;
    case Datatype::StringUtf16:
      return TILEDB_STRING_UTF16;
    case Datatype::StringUtf32:
      return TILEDB_STRING_UTF32;
    case Datatype::StringUcs2:
      return TILEDB_STRING_UCS2;
    case Datatype::StringUcs4:
      return TILEDB_STRING_UCS4;
    case Datatype::Any:
      return TILEDB_ANY;
    case Datatype::DateTimeYear:
      return TILEDB_DATETIME_YEAR;
    case Datatype::DateTimeMonth:
      return TILEDB_DATETIME_MONTH;
    case Datatype::DateTimeWeek:
      return TILEDB_DATETIME_WEEK;
    case Datatype::DateTimeDay:
      return TILEDB_DATETIME_DAY;
    case Datatype::DateTimeHour:
      return TILEDB_DATETIME_HR;
    case Datatype::DateTimeMinute:
      return TILEDB_DATETIME_MIN;
    case Datatype::DateTimeSecond:
      return TILEDB_DATETIME_SEC;
    case Datatype::DateTimeMillisecond:
      return TILEDB_DATETIME_MS;
    case Datatype::DateTimeMicrosecond:
      return TILEDB_DATETIME_US;
    case Datatype::DateTimeNanosecond:
      return TILEDB_DATETIME_NS;
    case Datatype::DateTimePicosecond:
      return TILEDB_DATETIME_PS;
    case Datatype::DateTimeFemtosecond:
      return TILEDB_DATETIME_FS;
    case Datatype::DateTimeAttosecond:
      return TILEDB_DATETIME_AS;
    case Datatype::TimeHour:
      return TILEDB_TIME_HR;
    case Datatype::TimeMinute:
      return TILEDB_TIME_MIN;
    case Datatype::TimeSecond:
      return TILEDB_TIME_SEC;
    case Datatype::TimeMillisecond:
      return TILEDB_TIME_MS;
    case Datatype::TimeMicrosecond:
      return TILEDB_TIME_US;
    case Datatype::TimeNanosecond:
      return TILEDB_TIME_NS;
    case Datatype::TimePicosecond:
      return TILEDB_TIME_PS;
    case Datatype::TimeFemtosecond:
      return TILEDB_TIME_FS;
    case Datatype::TimeAttosecond:
      return TILEDB_TIME_AS;
    case Datatype::Blob:
      return TILEDB_BLOB;
    case Datatype::Boolean:
      return TILEDB_BOOL;
    case Datatype::GeometryWkb:
      return TILEDB_GEOM_WKB;
    case Datatype::GeometryWkt:
      return TILEDB_GEOM_WKT;
    default:
      throw TileDBError("Invalid Datatype for conversion.");
  }
}

Datatype to_rs_datatype(tiledb_datatype_t dt) {
  switch (dt) {
    case TILEDB_INT32:
      return Datatype::Int32;
    case TILEDB_INT64:
      return Datatype::Int64;
    case TILEDB_FLOAT32:
      return Datatype::Float32;
    case TILEDB_FLOAT64:
      return Datatype::Float64;
    case TILEDB_CHAR:
      return Datatype::Char;
    case TILEDB_INT8:
      return Datatype::Int8;
    case TILEDB_UINT8:
      return Datatype::UInt8;
    case TILEDB_INT16:
      return Datatype::Int16;
    case TILEDB_UINT16:
      return Datatype::UInt16;
    case TILEDB_UINT32:
      return Datatype::UInt32;
    case TILEDB_UINT64:
      return Datatype::UInt64;
    case TILEDB_STRING_ASCII:
      return Datatype::StringAscii;
    case TILEDB_STRING_UTF8:
      return Datatype::StringUtf8;
    case TILEDB_STRING_UTF16:
      return Datatype::StringUtf16;
    case TILEDB_STRING_UTF32:
      return Datatype::StringUtf32;
    case TILEDB_STRING_UCS2:
      return Datatype::StringUcs2;
    case TILEDB_STRING_UCS4:
      return Datatype::StringUcs4;
    case TILEDB_ANY:
      return Datatype::Any;
    case TILEDB_DATETIME_YEAR:
      return Datatype::DateTimeYear;
    case TILEDB_DATETIME_MONTH:
      return Datatype::DateTimeMonth;
    case TILEDB_DATETIME_WEEK:
      return Datatype::DateTimeWeek;
    case TILEDB_DATETIME_DAY:
      return Datatype::DateTimeDay;
    case TILEDB_DATETIME_HR:
      return Datatype::DateTimeHour;
    case TILEDB_DATETIME_MIN:
      return Datatype::DateTimeMinute;
    case TILEDB_DATETIME_SEC:
      return Datatype::DateTimeSecond;
    case TILEDB_DATETIME_MS:
      return Datatype::DateTimeMillisecond;
    case TILEDB_DATETIME_US:
      return Datatype::DateTimeMicrosecond;
    case TILEDB_DATETIME_NS:
      return Datatype::DateTimeNanosecond;
    case TILEDB_DATETIME_PS:
      return Datatype::DateTimePicosecond;
    case TILEDB_DATETIME_FS:
      return Datatype::DateTimeFemtosecond;
    case TILEDB_DATETIME_AS:
      return Datatype::DateTimeAttosecond;
    case TILEDB_TIME_HR:
      return Datatype::TimeHour;
    case TILEDB_TIME_MIN:
      return Datatype::TimeMinute;
    case TILEDB_TIME_SEC:
      return Datatype::TimeSecond;
    case TILEDB_TIME_MS:
      return Datatype::TimeMillisecond;
    case TILEDB_TIME_US:
      return Datatype::TimeMicrosecond;
    case TILEDB_TIME_NS:
      return Datatype::TimeNanosecond;
    case TILEDB_TIME_PS:
      return Datatype::TimePicosecond;
    case TILEDB_TIME_FS:
      return Datatype::TimeFemtosecond;
    case TILEDB_TIME_AS:
      return Datatype::TimeAttosecond;
    case TILEDB_BLOB:
      return Datatype::Blob;
    case TILEDB_BOOL:
      return Datatype::Boolean;
    case TILEDB_GEOM_WKB:
      return Datatype::GeometryWkb;
    case TILEDB_GEOM_WKT:
      return Datatype::GeometryWkt;
    default:
      throw TileDBError("Invalid tiledb_datatype_t for conversion.");
  }
}

}  // namespace tiledb::rs
