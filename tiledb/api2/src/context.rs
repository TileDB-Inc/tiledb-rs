use tiledb_sys2::filesystem::FileSystem;

use crate::config::Config;
use crate::error::TileDBError;

use tiledb_sys2::context as ffi;

pub struct Context {
    ctx: cxx::UniquePtr<ffi::Context>,
}

impl Context {
    pub fn new() -> Result<Self, TileDBError> {
        Ok(Self {
            ctx: ffi::create_context()?,
        })
    }

    pub fn from_config(cfg: &Config) -> Result<Self, TileDBError> {
        Ok(Self {
            ctx: ffi::create_context_with_config(&(cfg.cfg))?,
        })
    }

    pub fn is_supported_fs(&self, fs: FileSystem) -> Result<bool, TileDBError> {
        Ok(self.ctx.is_supported_fs(fs as i32)?)
    }

    pub fn set_tag<K: AsRef<str>>(
        &self,
        key: K,
        val: K,
    ) -> Result<(), TileDBError> {
        Ok(self.ctx.set_tag(key.as_ref(), val.as_ref())?)
    }

    pub fn stats(&self) -> Result<String, TileDBError> {
        Ok(self.ctx.stats()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_context() -> Result<(), TileDBError> {
        assert!(Context::new().is_ok());
        Ok(())
    }

    #[test]
    fn create_with_config() -> Result<(), TileDBError> {
        let cfg = Config::new()?;
        assert!(Context::from_config(&cfg).is_ok());
        Ok(())
    }

    #[test]
    fn memfs_support() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        assert!(ctx.is_supported_fs(FileSystem::MemFs)?);
        Ok(())
    }

    #[test]
    fn hdfs_support() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        assert!(ctx.is_supported_fs(FileSystem::Hdfs).is_ok());
        Ok(())
    }

    #[test]
    fn set_tag() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        assert!(ctx.set_tag("foo", "bar").is_ok());
        Ok(())
    }

    #[test]
    fn stats() -> Result<(), TileDBError> {
        let ctx = Context::new()?;
        assert!(ctx.stats().is_ok());
        Ok(())
    }
}
