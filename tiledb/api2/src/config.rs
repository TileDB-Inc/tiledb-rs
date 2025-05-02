use crate::error::TileDBError;

#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    unsafe extern "C++" {
        include!("tiledb-api2/cpp/config.h");

        type Config;

        fn create_config() -> Result<UniquePtr<Config>>;
        fn get(&self, key: &str) -> Result<String>;
        fn contains(&self, key: &str) -> Result<bool>;
        fn set(&self, key: &str, val: &str) -> Result<()>;
        fn unset(&self, key: &str) -> Result<()>;
        fn load_from_file(&self, path: &str) -> Result<()>;
        fn save_to_file(&self, path: &str) -> Result<()>;
    }
}

pub struct Config {
    cfg: cxx::UniquePtr<ffi::Config>,
}

impl Config {
    pub fn new() -> Result<Self, TileDBError> {
        Ok(Self {
            cfg: ffi::create_config()?,
        })
    }

    pub fn get<K: AsRef<str>>(&self, key: K) -> Result<String, TileDBError> {
        Ok(self.cfg.get(key.as_ref())?)
    }

    pub fn contains<K: AsRef<str>>(&self, key: K) -> Result<bool, TileDBError> {
        Ok(self.cfg.contains(key.as_ref())?)
    }

    pub fn set<K: AsRef<str>>(
        &self,
        key: K,
        val: K,
    ) -> Result<(), TileDBError> {
        Ok(self.cfg.set(key.as_ref(), val.as_ref())?)
    }

    pub fn unset<K: AsRef<str>>(&self, key: K) -> Result<(), TileDBError> {
        Ok(self.cfg.unset(key.as_ref())?)
    }

    pub fn load_from_file<P: AsRef<str>>(
        &self,
        path: P,
    ) -> Result<(), TileDBError> {
        Ok(self.cfg.load_from_file(path.as_ref())?)
    }

    pub fn save_to_file<P: AsRef<str>>(
        &self,
        path: P,
    ) -> Result<(), TileDBError> {
        Ok(self.cfg.save_to_file(path.as_ref())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_test() -> Result<(), TileDBError> {
        let _ = Config::new()?;

        Ok(())
    }

    #[test]
    fn get_default() -> Result<(), TileDBError> {
        let cfg = Config::new()?;

        let val = cfg.get("rest.server_address")?;
        assert_eq!(val, "https://api.tiledb.com");

        Ok(())
    }

    #[test]
    fn set_and_get() -> Result<(), TileDBError> {
        let cfg = Config::new()?;

        let val = cfg.get("rest.server_address")?;
        assert_eq!(val, "https://api.tiledb.com");

        cfg.set("rest.server_address", "https://google.com")?;

        let val = cfg.get("rest.server_address")?;
        assert_eq!(val, "https://google.com");

        Ok(())
    }

    #[test]
    fn set_and_unset() -> Result<(), TileDBError> {
        let cfg = Config::new()?;

        cfg.set("xkcd", "some_value")?;
        let val = cfg.get("xkcd")?;

        assert_eq!(val, "some_value");

        cfg.unset("xkcd")?;
        let val = cfg.get("xkcd");
        assert!(val.is_err());

        Ok(())
    }
}
