pub use ffi::Datatype;

pub trait DomainType {
    const DATATYPE: Datatype;

    type CApiType;

    fn as_capi(&self) -> Self::CApiType;
}

impl DomainType for i32 {
    const DATATYPE: Datatype = Datatype::Int32;

    type CApiType = std::ffi::c_int;

    fn as_capi(&self) -> Self::CApiType {
        *self as Self::CApiType
    }
}
