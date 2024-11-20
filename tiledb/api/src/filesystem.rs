#[derive(Clone, Debug, PartialEq)]
pub enum Filesystem {
    Hdfs,
    S3,
    Azure,
    Gcs,
    Memfs,
}

#[derive(Debug)]
pub struct ParseFilesystemError;

impl Filesystem {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_filesystem_t {
        let ffi_enum = match *self {
            Filesystem::Hdfs => ffi::tiledb_filesystem_t_TILEDB_HDFS,
            Filesystem::S3 => ffi::tiledb_filesystem_t_TILEDB_S3,
            Filesystem::Azure => ffi::tiledb_filesystem_t_TILEDB_AZURE,
            Filesystem::Gcs => ffi::tiledb_filesystem_t_TILEDB_GCS,
            Filesystem::Memfs => ffi::tiledb_filesystem_t_TILEDB_MEMFS,
        };
        ffi_enum as ffi::tiledb_filesystem_t
    }

    pub fn to_string(&self) -> Option<String> {
        let mut c_str = out_ptr!();
        let res = unsafe {
            ffi::tiledb_filesystem_to_str(self.capi_enum(), &mut c_str)
        };
        if res == ffi::TILEDB_OK {
            let c_msg = unsafe { std::ffi::CStr::from_ptr(c_str) };
            Some(String::from(c_msg.to_string_lossy()))
        } else {
            None
        }
    }

    pub fn from_string(fs: &str) -> Result<Filesystem, ParseFilesystemError> {
        let c_fs = std::ffi::CString::new(fs).expect("Error creating CString");
        let mut c_ret: u32 = 0;
        let res = unsafe {
            ffi::tiledb_filesystem_from_str(
                c_fs.as_c_str().as_ptr(),
                &mut c_ret,
            )
        };

        if res == ffi::TILEDB_OK {
            // SAFETY: `c_ret` came from core as a valid filesystem
            Ok(Filesystem::try_from(c_ret).unwrap())
        } else {
            Err(ParseFilesystemError)
        }
    }
}

impl TryFrom<u32> for Filesystem {
    type Error = FilesystemFFIError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            ffi::tiledb_filesystem_t_TILEDB_HDFS => Ok(Filesystem::Hdfs),
            ffi::tiledb_filesystem_t_TILEDB_S3 => Ok(Filesystem::S3),
            ffi::tiledb_filesystem_t_TILEDB_AZURE => Ok(Filesystem::Azure),
            ffi::tiledb_filesystem_t_TILEDB_GCS => Ok(Filesystem::Gcs),
            ffi::tiledb_filesystem_t_TILEDB_MEMFS => Ok(Filesystem::Memfs),
            _ => Err(FilesystemFFIError::InvalidDiscriminant(value as u64)),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum FilesystemFFIError {
    #[error("Invalid discriminant for {}: {0}", std::any::type_name::<Filesystem>())]
    InvalidDiscriminant(u64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filesystem_roundtrips() {
        for i in 0..256 {
            let maybe_fs = Filesystem::try_from(i);
            if maybe_fs.is_ok() {
                let fs = maybe_fs.unwrap();
                let fs_str = fs.to_string().expect("Error creating string.");
                let str_fs = Filesystem::from_string(&fs_str)
                    .expect("Error round tripping filesystem string.");
                assert_eq!(str_fs, fs);
            }
        }
    }
}
