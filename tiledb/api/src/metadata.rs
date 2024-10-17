use tiledb_common::datatype::Datatype;
use tiledb_common::physical_type_go;

pub use tiledb_common::metadata::*;
pub use tiledb_common::metadata_value_go;

pub(crate) fn metadata_to_ffi(
    metadata: &Metadata,
) -> (u32, *const std::ffi::c_void, ffi::tiledb_datatype_t) {
    let (vec_size, vec_ptr) =
        metadata_value_go!(metadata.value, _DT, ref contents, {
            (contents.len(), contents.as_ptr() as *const std::ffi::c_void)
        });

    let c_datatype = ffi::tiledb_datatype_t::from(metadata.datatype);
    (vec_size as u32, vec_ptr, c_datatype)
}

pub(crate) fn metadata_from_ffi(
    key: String,
    datatype: Datatype,
    ffi: (u32, *const std::ffi::c_void),
) -> Metadata {
    let value = physical_type_go!(datatype, DT, {
        let slice = {
            let vec_ptr = if ffi.0 == 0 {
                std::ptr::NonNull::<DT>::dangling().as_ptr()
                    as *const std::ffi::c_void
            } else {
                ffi.1
            };
            unsafe {
                std::slice::from_raw_parts(vec_ptr as *const DT, ffi.0 as usize)
            }
        };
        Value::from(slice.to_vec())
    });

    Metadata {
        key,
        datatype,
        value,
    }
}
