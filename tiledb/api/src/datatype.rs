#[macro_export]
macro_rules! fn_typed {
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
