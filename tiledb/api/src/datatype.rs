#[macro_export]

/// Apply a generic function `$func` to data which implements `$datatype` and then run
/// the expression `$then` on the result.
/// The `$then` expression may use the function name as an identifier for the function result.
///
/// Variants:
/// - fn_typed!(my_function, my_datatype, arg1, ..., argN => then_expr)
///   Calls the function on the supplied arguments with a generic type parameter, and afterwards
///   runs `then_expr` on the result. The result is bound to an identifier which shadows the
///   function name.
/// - fn_typed!(obj.my_function, my_datatype, arg1, ..., argN => then_expr)
///   Calls the method on the supplied arguments with a generic type parameter, and afterwards
///   runs `then_expr` on the result. The result is bound to an identifier which shadows the
///   method name.
/// - fn_typed!(my_datatype, TypeName, then_expr)
///   Binds the type which implements `my_datatype` to `TypeName` for use in `then_expr`.

// note to developers: this is mimicking the C++ code
//      template <class Fn, class... Args>
//      inline auto apply_with_type(Fn&& f, Datatype type, Args&&... args)
//
// Also we probably only need the third variation since that can easily implement the other ones
//
macro_rules! fn_typed {
    ($datatype:expr, $typename:ident, $then:expr) => {{
        type Datatype = $crate::Datatype;
        match $datatype {
            Datatype::Int8 => { type $typename = i8; $then },
            Datatype::Int16 => { type $typename = i16; $then },
            Datatype::Int32 => { type $typename = i32; $then },
            Datatype::Int64 => { type $typename = i64; $then },
            Datatype::UInt8 => { type $typename = u8; $then },
            Datatype::UInt16 => { type $typename = u16; $then },
            Datatype::UInt32 => { type $typename = u32; $then },
            Datatype::UInt64 => { type $typename = u64; $then },
            Datatype::Float32 => { type $typename = f32; $then },
            Datatype::Float64 => { type $typename = f64; $then },
            Datatype::Char => unimplemented!(),
            Datatype::StringAscii => unimplemented!(),
            Datatype::StringUtf8 => unimplemented!(),
            Datatype::StringUtf16 => unimplemented!(),
            Datatype::StringUtf32 => unimplemented!(),
            Datatype::StringUcs2 => unimplemented!(),
            Datatype::StringUcs4 => unimplemented!(),
            Datatype::Any => unimplemented!(),
            Datatype::DateTimeYear => unimplemented!(),
            Datatype::DateTimeMonth => unimplemented!(),
            Datatype::DateTimeWeek => unimplemented!(),
            Datatype::DateTimeDay => unimplemented!(),
            Datatype::DateTimeHour => unimplemented!(),
            Datatype::DateTimeMinute => unimplemented!(),
            Datatype::DateTimeSecond => unimplemented!(),
            Datatype::DateTimeMillisecond => unimplemented!(),
            Datatype::DateTimeMicrosecond => unimplemented!(),
            Datatype::DateTimeNanosecond => unimplemented!(),
            Datatype::DateTimePicosecond => unimplemented!(),
            Datatype::DateTimeFemtosecond => unimplemented!(),
            Datatype::DateTimeAttosecond => unimplemented!(),
            Datatype::TimeHour => unimplemented!(),
            Datatype::TimeMinute => unimplemented!(),
            Datatype::TimeSecond => unimplemented!(),
            Datatype::TimeMillisecond => unimplemented!(),
            Datatype::TimeMicrosecond => unimplemented!(),
            Datatype::TimeNanosecond => unimplemented!(),
            Datatype::TimePicosecond => unimplemented!(),
            Datatype::TimeFemtosecond => unimplemented!(),
            Datatype::TimeAttosecond => unimplemented!(),
            Datatype::Blob => unimplemented!(),
            Datatype::Boolean => unimplemented!(),
            Datatype::GeometryWkb => unimplemented!(),
            Datatype::GeometryWkt => unimplemented!(),
        }
    }};

    ($func:ident, $datatype:expr$(, $arg:expr)* => $then:expr) => {{
        type Datatype = $crate::Datatype;
        match $datatype {
            Datatype::Int8 => {
                let $func = $func::<i8>($($arg,)*);
                $then
            }
            Datatype::Int16 => {
                let $func = $func::<i16>($($arg,)*);
                $then
            }
            Datatype::Int32 => {
                let $func = $func::<i32>($($arg,)*);
                $then
            }
            Datatype::Int64 => {
                let $func = $func::<i64>($($arg,)*);
                $then
            }
            Datatype::UInt8 => {
                let $func = $func::<u8>($($arg,)*);
                $then
            }
            Datatype::UInt16 => {
                let $func = $func::<u16>($($arg,)*);
                $then
            }
            Datatype::UInt32 => {
                let $func = $func::<u32>($($arg,)*);
                $then
            }
            Datatype::UInt64 => {
                let $func = $func::<u64>($($arg,)*);
                $then
            }
            Datatype::Float32 => {
                let $func = $func::<f32>($($arg,)*);
                $then
            }
            Datatype::Float64 => {
                let $func = $func::<f64>($($arg,)*);
                $then
            }
            Datatype::Char => unimplemented!(),
            Datatype::StringAscii => unimplemented!(),
            Datatype::StringUtf8 => unimplemented!(),
            Datatype::StringUtf16 => unimplemented!(),
            Datatype::StringUtf32 => unimplemented!(),
            Datatype::StringUcs2 => unimplemented!(),
            Datatype::StringUcs4 => unimplemented!(),
            Datatype::Any => unimplemented!(),
            Datatype::DateTimeYear => unimplemented!(),
            Datatype::DateTimeMonth => unimplemented!(),
            Datatype::DateTimeWeek => unimplemented!(),
            Datatype::DateTimeDay => unimplemented!(),
            Datatype::DateTimeHour => unimplemented!(),
            Datatype::DateTimeMinute => unimplemented!(),
            Datatype::DateTimeSecond => unimplemented!(),
            Datatype::DateTimeMillisecond => unimplemented!(),
            Datatype::DateTimeMicrosecond => unimplemented!(),
            Datatype::DateTimeNanosecond => unimplemented!(),
            Datatype::DateTimePicosecond => unimplemented!(),
            Datatype::DateTimeFemtosecond => unimplemented!(),
            Datatype::DateTimeAttosecond => unimplemented!(),
            Datatype::TimeHour => unimplemented!(),
            Datatype::TimeMinute => unimplemented!(),
            Datatype::TimeSecond => unimplemented!(),
            Datatype::TimeMillisecond => unimplemented!(),
            Datatype::TimeMicrosecond => unimplemented!(),
            Datatype::TimeNanosecond => unimplemented!(),
            Datatype::TimePicosecond => unimplemented!(),
            Datatype::TimeFemtosecond => unimplemented!(),
            Datatype::TimeAttosecond => unimplemented!(),
            Datatype::Blob => unimplemented!(),
            Datatype::Boolean => unimplemented!(),
            Datatype::GeometryWkb => unimplemented!(),
            Datatype::GeometryWkt => unimplemented!(),
        }
    }};
    ($obj:ident.$func:ident, $datatype:expr$(, $arg:expr)* => $then:expr) => {{
        type Datatype = $crate::Datatype;
        match $datatype {
            Datatype::Int8 => {
                let $func = $obj.$func::<i8>($($arg,)*);
                $then
            }
            Datatype::Int16 => {
                let $func = $obj.$func::<i16>($($arg,)*);
                $then
            }
            Datatype::Int32 => {
                let $func = $obj.$func::<i32>($($arg,)*);
                $then
            }
            Datatype::Int64 => {
                let $func = $obj.$func::<i64>($($arg,)*);
                $then
            }
            Datatype::UInt8 => {
                let $func = $obj.$func::<u8>($($arg,)*);
                $then
            }
            Datatype::UInt16 => {
                let $func = $obj.$func::<u16>($($arg,)*);
                $then
            }
            Datatype::UInt32 => {
                let $func = $obj.$func::<u32>($($arg,)*);
                $then
            }
            Datatype::UInt64 => {
                let $func = $obj.$func::<u64>($($arg,)*);
                $then
            }
            Datatype::Float32 => {
                let $func = $obj.$func::<f32>($($arg,)*);
                $then
            }
            Datatype::Float64 => {
                let $func = $obj.$func::<f64>($($arg,)*);
                $then
            }
            Datatype::Char => unimplemented!(),
            Datatype::StringAscii => unimplemented!(),
            Datatype::StringUtf8 => unimplemented!(),
            Datatype::StringUtf16 => unimplemented!(),
            Datatype::StringUtf32 => unimplemented!(),
            Datatype::StringUcs2 => unimplemented!(),
            Datatype::StringUcs4 => unimplemented!(),
            Datatype::Any => unimplemented!(),
            Datatype::DateTimeYear => unimplemented!(),
            Datatype::DateTimeMonth => unimplemented!(),
            Datatype::DateTimeWeek => unimplemented!(),
            Datatype::DateTimeDay => unimplemented!(),
            Datatype::DateTimeHour => unimplemented!(),
            Datatype::DateTimeMinute => unimplemented!(),
            Datatype::DateTimeSecond => unimplemented!(),
            Datatype::DateTimeMillisecond => unimplemented!(),
            Datatype::DateTimeMicrosecond => unimplemented!(),
            Datatype::DateTimeNanosecond => unimplemented!(),
            Datatype::DateTimePicosecond => unimplemented!(),
            Datatype::DateTimeFemtosecond => unimplemented!(),
            Datatype::DateTimeAttosecond => unimplemented!(),
            Datatype::TimeHour => unimplemented!(),
            Datatype::TimeMinute => unimplemented!(),
            Datatype::TimeSecond => unimplemented!(),
            Datatype::TimeMillisecond => unimplemented!(),
            Datatype::TimeMicrosecond => unimplemented!(),
            Datatype::TimeNanosecond => unimplemented!(),
            Datatype::TimePicosecond => unimplemented!(),
            Datatype::TimeFemtosecond => unimplemented!(),
            Datatype::TimeAttosecond => unimplemented!(),
            Datatype::Blob => unimplemented!(),
            Datatype::Boolean => unimplemented!(),
            Datatype::GeometryWkb => unimplemented!(),
            Datatype::GeometryWkt => unimplemented!(),
        }
    }};
}
