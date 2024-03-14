pub use ffi::Datatype;

pub trait DomainType {
    const DATATYPE: Datatype;

    type CApiType;

    fn as_capi(&self) -> Self::CApiType;
    fn from_capi(capi: &Self::CApiType) -> Self;
}

impl DomainType for i32 {
    const DATATYPE: Datatype = Datatype::Int32;

    type CApiType = std::ffi::c_int;

    fn as_capi(&self) -> Self::CApiType {
        *self as Self::CApiType
    }

    fn from_capi(capi: &Self::CApiType) -> Self {
        *capi as Self
    }
}

impl DomainType for f64 {
    const DATATYPE: Datatype = Datatype::Float64;

    type CApiType = std::ffi::c_double;

    fn as_capi(&self) -> Self::CApiType {
        *self as Self::CApiType
    }

    fn from_capi(capi: &Self::CApiType) -> Self {
        *capi as Self
    }
}
