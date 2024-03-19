pub trait CAPIConverter {
    type CAPIType: Default + Copy;

    fn to_capi(&self) -> Self::CAPIType;
    fn to_rust(value: &Self::CAPIType) -> Self;
}

impl CAPIConverter for i32 {
    type CAPIType = std::ffi::c_int;

    fn to_capi(&self) -> Self::CAPIType {
        *self as Self::CAPIType
    }

    fn to_rust(value: &Self::CAPIType) -> Self {
        *value as Self
    }
}

impl CAPIConverter for u32 {
    type CAPIType = std::ffi::c_uint;

    fn to_capi(&self) -> Self::CAPIType {
        *self as Self::CAPIType
    }

    fn to_rust(value: &Self::CAPIType) -> Self {
        *value as Self
    }
}

impl CAPIConverter for f64 {
    type CAPIType = std::ffi::c_double;

    fn to_capi(&self) -> Self::CAPIType {
        *self as Self::CAPIType
    }

    fn to_rust(value: &Self::CAPIType) -> Self {
        *value as Self
    }
}
