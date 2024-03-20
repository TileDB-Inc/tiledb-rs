use std::ops::Deref;

pub use ffi::VFSMode;

use crate::config::Config;
use crate::context::Context;
use crate::Result as TileDBResult;

pub(crate) enum RawVFS {
    Owned(*mut ffi::tiledb_vfs_t),
}

impl Deref for RawVFS {
    type Target = *mut ffi::tiledb_vfs_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawVFS::Owned(ref ffi) => ffi,
        }
    }
}

impl Drop for RawVFS {
    fn drop(&mut self) {
        let RawVFS::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_vfs_free(ffi) };
    }
}

pub struct VFS<'ctx> {
    context: &'ctx Context,
    raw: RawVFS,
}

pub(crate) enum RawVFSHandle {
    Owned(*mut ffi::tiledb_vfs_fh_t),
}

impl Deref for RawVFSHandle {
    type Target = *mut ffi::tiledb_vfs_fh_t;
    fn deref(&self) -> &Self::Target {
        match *self {
            RawVFSHandle::Owned(ref ffi) => ffi,
        }
    }
}

impl Drop for RawVFSHandle {
    fn drop(&mut self) {
        let RawVFSHandle::Owned(ref mut ffi) = *self;
        unsafe { ffi::tiledb_vfs_fh_free(ffi) };
    }
}

pub struct VFSHandle<'ctx> {
    context: &'ctx Context,
    raw: RawVFSHandle,
}

impl<'ctx> VFS<'ctx> {
    pub fn new(ctx: &'ctx Context, config: &Config) -> TileDBResult<VFS<'ctx>> {
        let mut c_vfs: *mut ffi::tiledb_vfs_t = out_ptr!();
        let res = unsafe {
            ffi::tiledb_vfs_alloc(
                ctx.as_mut_ptr(),
                config.as_mut_ptr(),
                &mut c_vfs,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(VFS {
                context: ctx,
                raw: RawVFS::Owned(c_vfs),
            })
        } else {
            Err(ctx.expect_last_error())
        }
    }

    pub fn get_config(&self) -> TileDBResult<Config> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let mut config = Config::default();
        let res = unsafe {
            ffi::tiledb_vfs_get_config(c_ctx, c_vfs, config.as_mut_ptr_ptr())
        };

        if res == ffi::TILEDB_OK {
            Ok(config)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn is_bucket(&self, uri: &str) -> TileDBResult<bool> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let mut c_is_bucket: i32 = 0;
        let res = unsafe {
            ffi::tiledb_vfs_is_empty_bucket(
                c_ctx,
                c_vfs,
                c_uri.as_ptr(),
                &mut c_is_bucket,
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(c_is_bucket == 1)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn is_empty_bucket(&self, uri: &str) -> TileDBResult<bool> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let mut c_is_empty: i32 = 0;
        let res = unsafe {
            ffi::tiledb_vfs_is_empty_bucket(
                c_ctx,
                c_vfs,
                c_uri.as_ptr(),
                &mut c_is_empty,
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(c_is_empty == 1)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn create_bucket(&self, uri: &str) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let res = unsafe {
            ffi::tiledb_vfs_create_bucket(c_ctx, c_vfs, c_uri.as_ptr())
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn remove_bucket(&self, uri: &str) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let res = unsafe {
            ffi::tiledb_vfs_remove_bucket(c_ctx, c_vfs, c_uri.as_ptr())
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn empty_bucket(&self, uri: &str) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let res = unsafe {
            ffi::tiledb_vfs_empty_bucket(c_ctx, c_vfs, c_uri.as_ptr())
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn is_dir(&self, uri: &str) -> TileDBResult<bool> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let mut c_is_dir: i32 = 0;
        let res = unsafe {
            ffi::tiledb_vfs_is_dir(c_ctx, c_vfs, c_uri.as_ptr(), &mut c_is_dir)
        };

        if res == ffi::TILEDB_OK {
            Ok(c_is_dir == 1)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn dir_size(&self, uri: &str) -> TileDBResult<u64> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let mut c_size: u64 = 0;
        let res = unsafe {
            ffi::tiledb_vfs_dir_size(c_ctx, c_vfs, c_uri.as_ptr(), &mut c_size)
        };

        if res == ffi::TILEDB_OK {
            Ok(c_size)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn create_dir(&self, uri: &str) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let res =
            unsafe { ffi::tiledb_vfs_create_dir(c_ctx, c_vfs, c_uri.as_ptr()) };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn remove_dir(&self, uri: &str) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let res =
            unsafe { ffi::tiledb_vfs_remove_dir(c_ctx, c_vfs, c_uri.as_ptr()) };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn copy_dir(&self, uri_src: &str, uri_tgt: &str) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri_src = cstring!(uri_src);
        let c_uri_tgt = cstring!(uri_tgt);
        let res = unsafe {
            ffi::tiledb_vfs_copy_dir(
                c_ctx,
                c_vfs,
                c_uri_src.as_ptr(),
                c_uri_tgt.as_ptr(),
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn move_dir(&self, uri_src: &str, uri_tgt: &str) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri_src = cstring!(uri_src);
        let c_uri_tgt = cstring!(uri_tgt);
        let res = unsafe {
            ffi::tiledb_vfs_move_dir(
                c_ctx,
                c_vfs,
                c_uri_src.as_ptr(),
                c_uri_tgt.as_ptr(),
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn is_file(&self, uri: &str) -> TileDBResult<bool> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let mut c_is_file: i32 = 0;
        let res = unsafe {
            ffi::tiledb_vfs_is_file(
                c_ctx,
                c_vfs,
                c_uri.as_ptr(),
                &mut c_is_file,
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(c_is_file == 1)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn file_size(&self, uri: &str) -> TileDBResult<u64> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let mut c_size: u64 = 0;
        let res = unsafe {
            ffi::tiledb_vfs_file_size(c_ctx, c_vfs, c_uri.as_ptr(), &mut c_size)
        };

        if res == ffi::TILEDB_OK {
            Ok(c_size)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn touch(&self, uri: &str) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let res =
            unsafe { ffi::tiledb_vfs_touch(c_ctx, c_vfs, c_uri.as_ptr()) };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn create_file(&self, uri: &str) -> TileDBResult<()> {
        self.touch(uri)
    }

    pub fn open(
        &self,
        uri: &str,
        mode: VFSMode,
    ) -> TileDBResult<VFSHandle<'ctx>> {
        let mut c_fh: *mut ffi::tiledb_vfs_fh_t = out_ptr!();
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let res = unsafe {
            ffi::tiledb_vfs_open(c_ctx, c_vfs, c_uri.as_ptr(), mode, &mut c_fh)
        };

        if res == ffi::TILEDB_OK {
            Ok(VFSHandle {
                context: self.context,
                raw: RawVFSHandle::Owned(c_fh),
            })
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn remove_file(&self, uri: &str) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let res = unsafe {
            ffi::tiledb_vfs_remove_file(c_ctx, c_vfs, c_uri.as_ptr())
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn copy_file(&self, uri_src: &str, uri_tgt: &str) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri_src = cstring!(uri_src);
        let c_uri_tgt = cstring!(uri_tgt);
        let res = unsafe {
            ffi::tiledb_vfs_copy_file(
                c_ctx,
                c_vfs,
                c_uri_src.as_ptr(),
                c_uri_tgt.as_ptr(),
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn move_file(&self, uri_src: &str, uri_tgt: &str) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri_src = cstring!(uri_src);
        let c_uri_tgt = cstring!(uri_tgt);
        let res = unsafe {
            ffi::tiledb_vfs_move_file(
                c_ctx,
                c_vfs,
                c_uri_src.as_ptr(),
                c_uri_tgt.as_ptr(),
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    /// # Safety
    /// This function is unsafe because of the data pointer being passed.
    pub unsafe fn ls(
        &self,
        uri: &str,
        callback: ffi::LSCallback,
        data: *mut ::std::os::raw::c_void,
    ) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let res = unsafe {
            ffi::tiledb_vfs_ls(c_ctx, c_vfs, c_uri.as_ptr(), callback, data)
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    /// # Safety
    /// This function is unsafe because of the data pointer being passed.
    pub unsafe fn ls_recursive(
        &self,
        uri: &str,
        callback: ffi::LSRecursiveCallback,
        data: *mut ::std::os::raw::c_void,
    ) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let res = unsafe {
            ffi::tiledb_vfs_ls_recursive(
                c_ctx,
                c_vfs,
                c_uri.as_ptr(),
                callback,
                data,
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }
}

impl<'ctx> VFSHandle<'ctx> {
    pub fn is_closed(&self) -> TileDBResult<bool> {
        let c_ctx = self.context.as_mut_ptr();
        let c_fh = *self.raw;
        let mut c_is_closed: i32 = 0;
        let res = unsafe {
            ffi::tiledb_vfs_fh_is_closed(c_ctx, c_fh, &mut c_is_closed)
        };

        if res == ffi::TILEDB_OK {
            Ok(c_is_closed == 1)
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn close(&self) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_fh = *self.raw;
        let res = unsafe { ffi::tiledb_vfs_close(c_ctx, c_fh) };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn read(&self, offset: u64, buffer: &mut [u8]) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_fh = *self.raw;
        let res = unsafe {
            ffi::tiledb_vfs_read(
                c_ctx,
                c_fh,
                offset,
                buffer.as_ptr() as *mut std::ffi::c_void,
                buffer.len() as u64,
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn write(&self, buffer: &[u8]) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_fh = *self.raw;
        let res = unsafe {
            ffi::tiledb_vfs_write(
                c_ctx,
                c_fh,
                buffer.as_ptr() as *const std::ffi::c_void,
                buffer.len() as u64,
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }

    pub fn sync(&self) -> TileDBResult<()> {
        let c_ctx = self.context.as_mut_ptr();
        let c_fh = *self.raw;
        let res = unsafe { ffi::tiledb_vfs_sync(c_ctx, c_fh) };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(self.context.expect_last_error())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error;
    use tempdir::TempDir;

    #[test]
    fn vfs_alloc() -> TileDBResult<()> {
        let ctx = Context::new()?;
        let cfg = Config::new()?;
        VFS::new(&ctx, &cfg)?;
        Ok(())
    }

    #[test]
    fn vfs_directory_operations() -> TileDBResult<()> {
        let ctx = Context::new()?;
        let cfg = Config::new()?;
        let vfs = VFS::new(&ctx, &cfg)?;

        let tmp_dir = TempDir::new("test_rs_bdelit").map_err(|e| {
            error::Error::from(format!(
                "Error creating temporary directory: {}",
                e
            ))
        })?;

        let tmp_uri = String::from("file://")
            + tmp_dir.path().to_str().expect("Error creating tmp_uri");

        assert!(vfs.is_dir(&tmp_uri)?);

        let dir1_uri = String::from("file://")
            + tmp_dir
                .path()
                .join("vfs_test_dir_1")
                .to_str()
                .expect("Whoops.");

        let dir1_foo_uri = String::from("file://")
            + tmp_dir
                .path()
                .join("vfs_test_dir_1/foo")
                .to_str()
                .expect("Whoops.");

        let dir2_uri = String::from("file://")
            + tmp_dir
                .path()
                .join("vfs_test_dir_2")
                .to_str()
                .expect("Whoops.");

        let dir3_uri = String::from("file://")
            + tmp_dir
                .path()
                .join("vfs_test_dir_3")
                .to_str()
                .expect("Whoops.");

        assert!(!vfs.is_dir(&dir1_uri)?);
        vfs.create_dir(&dir1_uri)?;
        assert!(vfs.is_dir(&dir1_uri)?);

        assert_eq!(vfs.dir_size(&dir1_uri)?, 0);

        vfs.touch(&dir1_foo_uri)?;
        assert!(vfs.is_file(&dir1_foo_uri)?);

        // Write some data for dir_size checks
        let data: &[u8] = &[42; 1024];
        let fh = vfs.open(&dir1_foo_uri, VFSMode::Write)?;
        fh.write(data)?;
        assert_eq!(vfs.dir_size(&dir1_uri)?, 1024);

        assert!(!vfs.is_dir(&dir2_uri)?);
        vfs.move_dir(&dir1_uri, &dir2_uri)?;
        assert!(!vfs.is_dir(&dir1_uri)?);
        assert!(vfs.is_dir(&dir2_uri)?);
        assert_eq!(vfs.dir_size(&dir2_uri)?, 1024);

        assert!(!vfs.is_dir(&dir3_uri)?);
        vfs.copy_dir(&dir2_uri, &dir3_uri)?;
        assert!(vfs.is_dir(&dir2_uri)?);
        assert!(vfs.is_dir(&dir3_uri)?);
        assert_eq!(vfs.dir_size(&dir2_uri)?, vfs.dir_size(&dir3_uri)?);
        assert_eq!(vfs.dir_size(&dir3_uri)?, 1024);

        vfs.remove_dir(&dir2_uri)?;
        vfs.remove_dir(&dir3_uri)?;
        assert!(!vfs.is_dir(&dir2_uri)?);
        assert!(!vfs.is_dir(&dir3_uri)?);

        Ok(())
    }

    #[test]
    fn vfs_file_operations() -> TileDBResult<()> {
        let ctx = Context::new()?;
        let cfg = Config::new()?;
        let vfs = VFS::new(&ctx, &cfg)?;

        let tmp_dir = TempDir::new("test_rs_bdelit").map_err(|e| {
            error::Error::from(format!(
                "Error creating temporary directory: {}",
                e
            ))
        })?;

        let file1_uri = String::from("file://")
            + tmp_dir
                .path()
                .join("vfs_test_file_1")
                .to_str()
                .expect("Whoops.");

        let file2_uri = String::from("file://")
            + tmp_dir
                .path()
                .join("vfs_test_file_2")
                .to_str()
                .expect("Whoops.");

        let file3_uri = String::from("file://")
            + tmp_dir
                .path()
                .join("vfs_test_file_3")
                .to_str()
                .expect("Whoops.");

        // A file doesn't exist before creation, but does after.
        assert!(!vfs.is_file(&file1_uri)?);
        vfs.touch(&file1_uri)?;
        assert!(vfs.is_file(&file1_uri)?);

        // Files are created empty.
        assert_eq!(vfs.file_size(&file1_uri)?, 0);

        // Move the file
        assert!(!vfs.is_file(&file2_uri)?);
        vfs.move_file(&file1_uri, &file2_uri)?;
        assert!(vfs.is_file(&file2_uri)?);
        assert_eq!(vfs.file_size(&file2_uri)?, 0);

        // Open the file and write some data to it.
        let mut data1 = String::from("Hello, world!");
        let fh1 = vfs.open(&file2_uri, VFSMode::Write)?;
        unsafe {
            fh1.write(data1.as_bytes_mut())?;
        }
        fh1.sync()?;
        fh1.close()?;
        assert!(fh1.is_closed()?);

        // Copy the file
        vfs.copy_file(&file2_uri, &file3_uri)?;
        assert_eq!(vfs.file_size(&file3_uri)?, 13);

        // Check that removing works
        vfs.remove_file(&file2_uri)?;
        assert!(!vfs.is_file(&file2_uri)?);

        // Check that reading from the copy matches the original write.
        let mut data2 = String::from("             ");
        let fh2 = vfs.open(&file3_uri, VFSMode::Read)?;
        unsafe {
            fh2.read(0, data2.as_bytes_mut())?;
        }
        assert_eq!(data2, data1);

        Ok(())
    }

    fn create_test_dir_structure(
        vfs: &VFS,
        tmp_dir: &TempDir,
    ) -> TileDBResult<()> {
        let tmp_uri = String::from("file://")
            + tmp_dir.path().to_str().expect("Error creating tmp_uri");

        assert!(vfs.is_dir(&tmp_uri)?);

        let dir1_uri = String::from("file://")
            + tmp_dir
                .path()
                .join("vfs_test_dir_1")
                .to_str()
                .expect("Whoops.");

        let dir1_foo_uri = String::from("file://")
            + tmp_dir
                .path()
                .join("vfs_test_dir_1/foo")
                .to_str()
                .expect("Whoops.");

        let dir2_uri = String::from("file://")
            + tmp_dir
                .path()
                .join("vfs_test_dir_2")
                .to_str()
                .expect("Whoops.");

        let dir3_uri = String::from("file://")
            + tmp_dir
                .path()
                .join("vfs_test_dir_3")
                .to_str()
                .expect("Whoops.");

        vfs.create_dir(&dir1_uri)?;
        vfs.create_dir(&dir2_uri)?;
        vfs.create_dir(&dir3_uri)?;

        let data: &[u8] = &[32; 1024];
        let fh = vfs.open(&dir1_foo_uri, VFSMode::Write)?;
        fh.write(data)?;

        Ok(())
    }

    unsafe extern "C" fn ls_callback(
        _: *const std::os::raw::c_char,
        count: *mut std::os::raw::c_void,
    ) -> i32 {
        *(count as *mut u64) += 1;
        1
    }

    #[test]
    fn vfs_ls() -> TileDBResult<()> {
        let ctx = Context::new()?;
        let cfg = Config::new()?;
        let vfs = VFS::new(&ctx, &cfg)?;

        let tmp_dir = TempDir::new("test_rs_bdelit").map_err(|e| {
            error::Error::from(format!(
                "Error creating temporary directory: {}",
                e
            ))
        })?;

        create_test_dir_structure(&vfs, &tmp_dir)?;

        let tmp_uri = tmp_dir.path().to_str().expect("Error getting temp dir");
        let mut count: u64 = 0;
        unsafe {
            vfs.ls(
                tmp_uri,
                Some(ls_callback),
                &mut count as *mut std::ffi::c_ulonglong
                    as *mut std::ffi::c_void,
            )?;
        }

        // ls only sees the three directories.
        assert_eq!(count, 3);

        Ok(())
    }

    unsafe extern "C" fn ls_recursive_callback(
        _: *const std::os::raw::c_char,
        _: usize,
        _: u64,
        count: *mut std::os::raw::c_void,
    ) -> i32 {
        *(count as *mut u64) += 1;
        1
    }

    #[test]
    fn vfs_ls_recursive_old() -> TileDBResult<()> {
        // Recursive ls over the Posix backend doesn't exist before 2.21
        let (major, minor, _) = crate::version();
        if major >= 2 && minor >= 21 {
            return Ok(());
        }

        let ctx = Context::new()?;
        let cfg = Config::new()?;
        let vfs = VFS::new(&ctx, &cfg)?;

        let tmp_dir = TempDir::new("test_rs_bdelit").map_err(|e| {
            error::Error::from(format!(
                "Error creating temporary directory: {}",
                e
            ))
        })?;

        let tmp_uri = tmp_dir.path().to_str().expect("Error getting tmp_uri");
        let mut count: u64 = 0;
        assert!(unsafe {
            vfs.ls_recursive(
                tmp_uri,
                Some(ls_recursive_callback),
                &mut count as *mut std::ffi::c_ulonglong
                    as *mut std::ffi::c_void,
            )
            .is_err()
        });

        Ok(())
    }

    #[test]
    fn vfs_ls_recursive_new() -> TileDBResult<()> {
        // Recursive ls over the Posix backend doesn't exist before 2.21
        let (major, minor, patch) = crate::version();
        println!("VERSION: {}.{}.{}", major, minor, patch);
        if !(major >= 2 && minor >= 21) {
            return Ok(());
        }

        let ctx = Context::new()?;
        let cfg = Config::new()?;
        let vfs = VFS::new(&ctx, &cfg)?;

        let tmp_dir = TempDir::new("test_rs_bdelit").map_err(|e| {
            error::Error::from(format!(
                "Error creating temporary directory: {}",
                e
            ))
        })?;

        create_test_dir_structure(&vfs, &tmp_dir)?;

        let tmp_uri = tmp_dir.path().to_str().expect("Error getting temp dir");
        let mut count: u64 = 0;
        unsafe {
            vfs.ls_recursive(
                tmp_uri,
                Some(ls_recursive_callback),
                &mut count as *mut std::ffi::c_ulonglong
                    as *mut std::ffi::c_void,
            )?;
        }

        // ls_recursive sees three directories and one file.
        assert_eq!(count, 4);

        Ok(())
    }
}
