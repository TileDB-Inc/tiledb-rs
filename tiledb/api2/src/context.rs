use crate::config::Config;
use crate::enums::FileSystem;
use crate::error::TileDBError;

#[cxx::bridge(namespace = "tiledb::rs")]
pub mod ffi {
    unsafe extern "C++" {
        include!("tiledb-api2/cpp/context.h");
        include!("tiledb-api2/cpp/enums.h");

        type Config = crate::config::ffi::Config;
        type Context;

        pub fn create_context() -> Result<UniquePtr<Context>>;
        pub fn create_context_with_config(
            cfg: &UniquePtr<Config>,
        ) -> Result<UniquePtr<Context>>;

        pub fn is_supported_fs(self: &Context, fs: i32) -> Result<bool>;
        pub fn set_tag(self: &Context, key: &str, val: &str) -> Result<()>;
        pub fn stats(self: &Context) -> Result<String>;
    }
}

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
