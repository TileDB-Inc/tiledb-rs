use crate::context::Context;
use crate::Datatype;
use crate::Result as TileDBResult;

pub struct Attribute {
    wrapped: *mut ffi::tiledb_attribute_t,
}

impl Attribute {
    pub fn new(
        context: &Context,
        name: &str,
        attr_type: &Datatype,
    ) -> TileDBResult<Attribute> {
        let mut c_att: *mut ffi::tiledb_attribute_t = std::ptr::null_mut();
        let c_datatype: ffi::tiledb_datatype_t = attr_type.capi_enum();
        let c_name = cstring!(name);
        if unsafe {
            ffi::tiledb_attribute_alloc(
                context.as_mut_ptr(),
                c_name.as_ptr(),
                c_datatype,
                &mut c_att,
            )
        } == ffi::TILEDB_OK
        {
            Ok(Attribute { wrapped: c_att })
        } else {
            Err(context.expect_last_error())
        }
    }

    pub(crate) fn as_mut_ptr(&self) -> *mut ffi::tiledb_attribute_t {
        self.wrapped
    }
}
