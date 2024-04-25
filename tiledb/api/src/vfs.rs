use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::config::{Config, RawConfig};
use crate::context::{CApiInterface, Context, ContextBound};
use crate::Result as TileDBResult;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum VFSMode {
    Read,
    Write,
    Append,
}

impl VFSMode {
    pub(crate) fn capi_enum(&self) -> ffi::tiledb_vfs_mode_t {
        match *self {
            VFSMode::Read => ffi::tiledb_vfs_mode_t_TILEDB_VFS_READ,
            VFSMode::Write => ffi::tiledb_vfs_mode_t_TILEDB_VFS_WRITE,
            VFSMode::Append => ffi::tiledb_vfs_mode_t_TILEDB_VFS_APPEND,
        }
    }
}

impl TryFrom<ffi::tiledb_vfs_mode_t> for VFSMode {
    type Error = crate::error::Error;
    fn try_from(value: ffi::tiledb_vfs_mode_t) -> TileDBResult<Self> {
        match value {
            ffi::tiledb_vfs_mode_t_TILEDB_VFS_READ => Ok(VFSMode::Read),
            ffi::tiledb_vfs_mode_t_TILEDB_VFS_WRITE => Ok(VFSMode::Write),
            ffi::tiledb_vfs_mode_t_TILEDB_VFS_APPEND => Ok(VFSMode::Append),
            _ => Err(Self::Error::LibTileDB(format!(
                "Invalid VFS mode: {}",
                value
            ))),
        }
    }
}

pub enum VFSLsStatus {
    Continue,
    Stop,
    Error,
}

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

#[derive(ContextBound)]
pub struct VFS<'ctx> {
    #[context]
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

#[derive(ContextBound)]
pub struct VFSHandle<'ctx> {
    #[context]
    context: &'ctx Context,
    raw: RawVFSHandle,
}

impl<'ctx> VFS<'ctx> {
    pub fn new(ctx: &'ctx Context, config: &Config) -> TileDBResult<VFS<'ctx>> {
        let c_config = config.capi();
        let mut c_vfs: *mut ffi::tiledb_vfs_t = out_ptr!();
        ctx.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_alloc(ctx, c_config, &mut c_vfs)
        })?;
        Ok(VFS {
            context: ctx,
            raw: RawVFS::Owned(c_vfs),
        })
    }

    pub fn get_config(&self) -> TileDBResult<Config> {
        let c_vfs = *self.raw;
        let mut c_cfg: *mut ffi::tiledb_config_t = out_ptr!();
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_get_config(ctx, c_vfs, &mut c_cfg)
        })?;

        Ok(Config {
            raw: RawConfig::Owned(c_cfg),
        })
    }

    pub fn is_bucket(&self, uri: &str) -> TileDBResult<bool> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let mut c_is_bucket: i32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_is_empty_bucket(
                ctx,
                c_vfs,
                c_uri.as_ptr(),
                &mut c_is_bucket,
            )
        })?;

        Ok(c_is_bucket == 1)
    }

    pub fn is_empty_bucket(&self, uri: &str) -> TileDBResult<bool> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let mut c_is_empty: i32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_is_empty_bucket(
                ctx,
                c_vfs,
                c_uri.as_ptr(),
                &mut c_is_empty,
            )
        })?;

        Ok(c_is_empty == 1)
    }

    pub fn create_bucket(&self, uri: &str) -> TileDBResult<()> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_create_bucket(ctx, c_vfs, c_uri.as_ptr())
        })?;

        Ok(())
    }

    pub fn remove_bucket(&self, uri: &str) -> TileDBResult<()> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_remove_bucket(ctx, c_vfs, c_uri.as_ptr())
        })?;

        Ok(())
    }

    pub fn empty_bucket(&self, uri: &str) -> TileDBResult<()> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_empty_bucket(ctx, c_vfs, c_uri.as_ptr())
        })?;

        Ok(())
    }

    pub fn is_dir(&self, uri: &str) -> TileDBResult<bool> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let mut c_is_dir: i32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_is_dir(ctx, c_vfs, c_uri.as_ptr(), &mut c_is_dir)
        })?;

        Ok(c_is_dir == 1)
    }

    pub fn dir_size(&self, uri: &str) -> TileDBResult<u64> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let mut c_size: u64 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_dir_size(ctx, c_vfs, c_uri.as_ptr(), &mut c_size)
        })?;

        Ok(c_size)
    }

    pub fn create_dir(&self, uri: &str) -> TileDBResult<()> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_create_dir(ctx, c_vfs, c_uri.as_ptr())
        })?;

        Ok(())
    }

    pub fn remove_dir(&self, uri: &str) -> TileDBResult<()> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_remove_dir(ctx, c_vfs, c_uri.as_ptr())
        })?;

        Ok(())
    }

    pub fn copy_dir(&self, uri_src: &str, uri_tgt: &str) -> TileDBResult<()> {
        let c_vfs = *self.raw;
        let c_uri_src = cstring!(uri_src);
        let c_uri_tgt = cstring!(uri_tgt);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_copy_dir(
                ctx,
                c_vfs,
                c_uri_src.as_ptr(),
                c_uri_tgt.as_ptr(),
            )
        })?;

        Ok(())
    }

    pub fn move_dir(&self, uri_src: &str, uri_tgt: &str) -> TileDBResult<()> {
        let c_vfs = *self.raw;
        let c_uri_src = cstring!(uri_src);
        let c_uri_tgt = cstring!(uri_tgt);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_move_dir(
                ctx,
                c_vfs,
                c_uri_src.as_ptr(),
                c_uri_tgt.as_ptr(),
            )
        })?;

        Ok(())
    }

    pub fn is_file(&self, uri: &str) -> TileDBResult<bool> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let mut c_is_file: i32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_is_file(ctx, c_vfs, c_uri.as_ptr(), &mut c_is_file)
        })?;

        Ok(c_is_file == 1)
    }

    pub fn file_size(&self, uri: &str) -> TileDBResult<u64> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        let mut c_size: u64 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_file_size(ctx, c_vfs, c_uri.as_ptr(), &mut c_size)
        })?;

        Ok(c_size)
    }

    pub fn touch(&self, uri: &str) -> TileDBResult<()> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_touch(ctx, c_vfs, c_uri.as_ptr())
        })?;

        Ok(())
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
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_open(
                ctx,
                c_vfs,
                c_uri.as_ptr(),
                mode.capi_enum(),
                &mut c_fh,
            )
        })?;

        Ok(VFSHandle {
            context: self.context,
            raw: RawVFSHandle::Owned(c_fh),
        })
    }

    pub fn remove_file(&self, uri: &str) -> TileDBResult<()> {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_remove_file(ctx, c_vfs, c_uri.as_ptr())
        })?;

        Ok(())
    }

    pub fn copy_file(&self, uri_src: &str, uri_tgt: &str) -> TileDBResult<()> {
        let c_vfs = *self.raw;
        let c_uri_src = cstring!(uri_src);
        let c_uri_tgt = cstring!(uri_tgt);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_copy_file(
                ctx,
                c_vfs,
                c_uri_src.as_ptr(),
                c_uri_tgt.as_ptr(),
            )
        })?;

        Ok(())
    }

    pub fn move_file(&self, uri_src: &str, uri_tgt: &str) -> TileDBResult<()> {
        let c_vfs = *self.raw;
        let c_uri_src = cstring!(uri_src);
        let c_uri_tgt = cstring!(uri_tgt);
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_move_file(
                ctx,
                c_vfs,
                c_uri_src.as_ptr(),
                c_uri_tgt.as_ptr(),
            )
        })?;

        Ok(())
    }

    pub fn ls<F>(&self, uri: &str, mut callback: F) -> TileDBResult<()>
    where
        F: FnMut(&str) -> VFSLsStatus,
    {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);

        // See the StackOverflow link on vfs_ls_cb_handler
        let mut cb: &mut dyn FnMut(&str) -> VFSLsStatus = &mut callback;
        let cb = &mut cb;

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_ls(
                ctx,
                c_vfs,
                c_uri.as_ptr(),
                Some(vfs_ls_cb_handler),
                cb as *mut _ as *mut std::ffi::c_void,
            )
        })?;

        Ok(())
    }

    pub fn ls_recursive<F>(
        &self,
        uri: &str,
        mut callback: F,
    ) -> TileDBResult<()>
    where
        F: FnMut(&str, u64) -> VFSLsStatus,
    {
        let c_vfs = *self.raw;
        let c_uri = cstring!(uri);

        // See the StackOverflow link on vfs_ls_recursive_cb_handler
        let mut cb: &mut dyn FnMut(&str, u64) -> VFSLsStatus = &mut callback;
        let cb = &mut cb;

        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_ls_recursive(
                ctx,
                c_vfs,
                c_uri.as_ptr(),
                Some(vfs_ls_recursive_cb_handler),
                cb as *mut _ as *mut std::ffi::c_void,
            )
        })?;

        Ok(())
    }
}

// This bit of complexity is based on the StackOverflow answer here:
// https://stackoverflow.com/a/32270215
extern "C" fn vfs_ls_cb_handler(
    path: *const ::std::os::raw::c_char,
    callback_data: *mut ::std::os::raw::c_void,
) -> std::ffi::c_int {
    let closure: &mut &mut dyn FnMut(&str) -> VFSLsStatus = unsafe {
        std::mem::transmute(
            // This complicated cast is brought to you by clippy. The original
            // did not require this, but the original is also two years old.
            &mut *(callback_data
                as *mut &mut dyn for<'a> std::ops::FnMut(
                    &'a str,
                )
                    -> VFSLsStatus),
        )
    };

    let c_str: &std::ffi::CStr = unsafe { std::ffi::CStr::from_ptr(path) };
    let slice = c_str.to_str();

    if slice.is_err() {
        return -1;
    }

    match closure(slice.unwrap()) {
        VFSLsStatus::Continue => 1,
        VFSLsStatus::Stop => 0,
        VFSLsStatus::Error => -1,
    }
}

// This bit of complexity is based on the StackOverflow answer here:
// https://stackoverflow.com/a/32270215
extern "C" fn vfs_ls_recursive_cb_handler(
    path: *const ::std::os::raw::c_uchar,
    path_len: usize,
    object_size: u64,
    callback_data: *mut ::std::os::raw::c_void,
) -> std::ffi::c_int {
    let closure: &mut &mut dyn FnMut(&str, u64) -> VFSLsStatus = unsafe {
        std::mem::transmute(
            // This complicated cast is brought to you by clippy. The original
            // did not require this, but the original is also two years old.
            &mut *(callback_data
                as *mut &mut dyn for<'a> std::ops::FnMut(
                    &'a str,
                    &'a u64,
                )
                    -> VFSLsStatus),
        )
    };

    let path_slice: &[u8] =
        unsafe { std::slice::from_raw_parts(path, path_len) };
    let c_str = std::str::from_utf8(path_slice);
    if c_str.is_err() {
        return -1;
    }

    match closure(c_str.unwrap(), object_size) {
        VFSLsStatus::Continue => 1,
        VFSLsStatus::Stop => 0,
        VFSLsStatus::Error => -1,
    }
}

impl<'ctx> VFSHandle<'ctx> {
    pub fn is_closed(&self) -> TileDBResult<bool> {
        let c_fh = *self.raw;
        let mut c_is_closed: i32 = 0;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_fh_is_closed(ctx, c_fh, &mut c_is_closed)
        })?;

        Ok(c_is_closed == 1)
    }

    pub fn close(&self) -> TileDBResult<()> {
        let c_fh = *self.raw;
        self.capi_call(|ctx| unsafe { ffi::tiledb_vfs_close(ctx, c_fh) })?;

        Ok(())
    }

    pub fn read(&self, offset: u64, buffer: &mut [u8]) -> TileDBResult<()> {
        let c_fh = *self.raw;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_read(
                ctx,
                c_fh,
                offset,
                buffer.as_ptr() as *mut std::ffi::c_void,
                buffer.len() as u64,
            )
        })?;

        Ok(())
    }

    pub fn write(&self, buffer: &[u8]) -> TileDBResult<()> {
        let c_fh = *self.raw;
        self.capi_call(|ctx| unsafe {
            ffi::tiledb_vfs_write(
                ctx,
                c_fh,
                buffer.as_ptr() as *const std::ffi::c_void,
                buffer.len() as u64,
            )
        })?;

        Ok(())
    }

    pub fn sync(&self) -> TileDBResult<()> {
        let c_fh = *self.raw;
        self.capi_call(|ctx| unsafe { ffi::tiledb_vfs_sync(ctx, c_fh) })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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

        let tmp_dir = TempDir::new().unwrap();

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

        let tmp_dir = TempDir::new().unwrap();

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

    #[test]
    fn vfs_ls() -> TileDBResult<()> {
        let ctx = Context::new()?;
        let cfg = Config::new()?;
        let vfs = VFS::new(&ctx, &cfg)?;

        let tmp_dir = TempDir::new().unwrap();

        create_test_dir_structure(&vfs, &tmp_dir)?;

        let tmp_uri = tmp_dir.path().to_str().expect("Error getting temp dir");
        let mut count: u64 = 0;
        let cb = |_: &str| -> VFSLsStatus {
            count += 1;
            VFSLsStatus::Continue
        };

        vfs.ls(tmp_uri, cb)?;

        // ls only sees the three directories.
        assert_eq!(count, 3);

        Ok(())
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

        let tmp_dir = TempDir::new().unwrap();

        let tmp_uri = tmp_dir.path().to_str().expect("Error getting tmp_uri");
        let mut count: u64 = 0;
        let cb = |_: &str, _: u64| -> VFSLsStatus {
            count += 1;
            VFSLsStatus::Continue
        };
        assert!(vfs.ls_recursive(tmp_uri, cb).is_err());

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

        let tmp_dir = TempDir::new().unwrap();

        create_test_dir_structure(&vfs, &tmp_dir)?;

        let tmp_uri = tmp_dir.path().to_str().expect("Error getting temp dir");
        let mut count: u64 = 0;
        let cb = |_: &str, _: u64| -> VFSLsStatus {
            count += 1;
            VFSLsStatus::Continue
        };
        vfs.ls_recursive(tmp_uri, cb)?;

        // ls_recursive sees three directories and one file.
        assert_eq!(count, 4);

        Ok(())
    }
}
