use std::ops::Deref;

use crate::context::{CApiError, CApiResult, RawError};

pub(crate) enum RawConfig {
    Owned(*mut ffi::tiledb_config_t),
}

impl Deref for RawConfig {
    type Target = *mut ffi::tiledb_config_t;
    fn deref(&self) -> &Self::Target {
        let RawConfig::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawConfig {
    fn drop(&mut self) {
        let RawConfig::Owned(ref mut ffi) = *self;
        unsafe {
            ffi::tiledb_config_free(ffi);
        }
    }
}

pub(crate) enum RawConfigIter {
    Owned(*mut ffi::tiledb_config_iter_t),
}

impl Deref for RawConfigIter {
    type Target = *mut ffi::tiledb_config_iter_t;
    fn deref(&self) -> &Self::Target {
        let RawConfigIter::Owned(ref ffi) = *self;
        ffi
    }
}

impl Drop for RawConfigIter {
    fn drop(&mut self) {
        let RawConfigIter::Owned(ref mut ffi) = *self;
        unsafe {
            ffi::tiledb_config_iter_free(ffi);
        }
    }
}

pub struct Config {
    pub(crate) raw: RawConfig,
}

pub struct ConfigIterator<'cfg> {
    pub(crate) _cfg: &'cfg Config,
    pub(crate) raw: RawConfigIter,
}

impl Config {
    pub fn capi(&self) -> *mut ffi::tiledb_config_t {
        *self.raw
    }

    pub fn new() -> CApiResult<Config> {
        let mut c_cfg: *mut ffi::tiledb_config_t = out_ptr!();
        let mut c_err: *mut ffi::tiledb_error_t = std::ptr::null_mut();
        let res = unsafe { ffi::tiledb_config_alloc(&mut c_cfg, &mut c_err) };
        if res == ffi::TILEDB_OK {
            Ok(Config {
                raw: RawConfig::Owned(c_cfg),
            })
        } else {
            Err(CApiError::from(RawError::Owned(c_err)))
        }
    }

    pub(crate) fn from_raw(raw: RawConfig) -> Self {
        Self { raw }
    }

    pub fn set<B>(&mut self, key: &str, val: B) -> CApiResult<()>
    where
        B: AsRef<[u8]>,
    {
        let c_key = cstring!(key);
        let c_val = cstring!(val.as_ref());

        let mut c_err: *mut ffi::tiledb_error_t = std::ptr::null_mut();
        let res = unsafe {
            ffi::tiledb_config_set(
                *self.raw,
                c_key.as_c_str().as_ptr(),
                c_val.as_c_str().as_ptr(),
                &mut c_err,
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(CApiError::from(RawError::Owned(c_err)))
        }
    }

    pub fn with<B>(self, key: &str, val: B) -> CApiResult<Self>
    where
        B: AsRef<[u8]>,
    {
        let mut s = self;
        s.set(key, val)?;
        Ok(s)
    }

    pub fn get(&self, key: &str) -> CApiResult<Option<String>> {
        let c_key =
            std::ffi::CString::new(key).expect("Error creating CString");
        let mut val = std::ptr::null::<std::os::raw::c_char>();
        let mut c_err: *mut ffi::tiledb_error_t = std::ptr::null_mut();
        let res = unsafe {
            ffi::tiledb_config_get(
                *self.raw,
                c_key.as_c_str().as_ptr(),
                &mut val as *mut *const std::os::raw::c_char,
                &mut c_err,
            )
        };
        if res == ffi::TILEDB_OK && !val.is_null() {
            let c_msg = unsafe { std::ffi::CStr::from_ptr(val) };
            Ok(Some(String::from(c_msg.to_string_lossy())))
        } else if res == ffi::TILEDB_OK {
            Ok(None)
        } else {
            Err(CApiError::from(RawError::Owned(c_err)))
        }
    }

    pub fn set_common_option(&mut self, opt: &CommonOption) -> CApiResult<()> {
        opt.apply(self)
    }

    pub fn with_common_option(self, opt: &CommonOption) -> CApiResult<Self> {
        let mut s = self;
        s.set_common_option(opt)?;
        Ok(s)
    }

    pub fn unset(&mut self, key: &str) -> CApiResult<()> {
        let c_key =
            std::ffi::CString::new(key).expect("Error creating CString");
        let mut c_err: *mut ffi::tiledb_error_t = std::ptr::null_mut();
        let res = unsafe {
            ffi::tiledb_config_unset(
                *self.raw,
                c_key.as_c_str().as_ptr(),
                &mut c_err,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(CApiError::from(RawError::Owned(c_err)))
        }
    }

    pub fn load(&mut self, path: &str) -> CApiResult<()> {
        let c_path =
            std::ffi::CString::new(path).expect("Error creating CString");
        let mut c_err: *mut ffi::tiledb_error_t = std::ptr::null_mut();
        let res = unsafe {
            ffi::tiledb_config_load_from_file(
                *self.raw,
                c_path.as_c_str().as_ptr(),
                &mut c_err,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(CApiError::from(RawError::Owned(c_err)))
        }
    }

    pub fn save(&mut self, path: &str) -> CApiResult<()> {
        let c_path =
            std::ffi::CString::new(path).expect("Error creating CString");
        let mut c_err: *mut ffi::tiledb_error_t = std::ptr::null_mut();
        let res = unsafe {
            ffi::tiledb_config_save_to_file(
                *self.raw,
                c_path.as_c_str().as_ptr(),
                &mut c_err,
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(CApiError::from(RawError::Owned(c_err)))
        }
    }
}

impl PartialEq for Config {
    fn eq(&self, other: &Self) -> bool {
        let mut eq: u8 = 0;
        let res = unsafe {
            ffi::tiledb_config_compare(*self.raw, *other.raw, &mut eq)
        };
        if res == ffi::TILEDB_OK {
            eq == 1
        } else {
            false
        }
    }
}

impl<'cfg> IntoIterator for &'cfg Config {
    type Item = (String, String);
    type IntoIter = ConfigIterator<'cfg>;

    fn into_iter(self) -> Self::IntoIter {
        let mut c_iter: *mut ffi::tiledb_config_iter_t = out_ptr!();
        let c_path = std::ptr::null::<std::os::raw::c_char>();
        let mut c_err: *mut ffi::tiledb_error_t = out_ptr!();
        let res = unsafe {
            ffi::tiledb_config_iter_alloc(
                *self.raw,
                c_path,
                &mut c_iter,
                &mut c_err,
            )
        };

        if res == ffi::TILEDB_OK {
            ConfigIterator {
                _cfg: self,
                raw: RawConfigIter::Owned(c_iter),
            }
        } else {
            panic!("Not entirely sure what to do here.")
        }
    }
}

impl Iterator for ConfigIterator<'_> {
    type Item = (String, String);
    fn next(&mut self) -> Option<Self::Item> {
        let mut c_key = std::ptr::null::<std::os::raw::c_char>();
        let mut c_val = std::ptr::null::<std::os::raw::c_char>();
        let mut c_err: *mut ffi::tiledb_error_t = std::ptr::null_mut();
        let mut done: i32 = 0;
        let res = unsafe {
            ffi::tiledb_config_iter_done(*self.raw, &mut done, &mut c_err)
        };

        if res != ffi::TILEDB_OK || done != 0 {
            return None;
        }

        let res = unsafe {
            ffi::tiledb_config_iter_here(
                *self.raw,
                &mut c_key as *mut *const std::os::raw::c_char,
                &mut c_val as *mut *const std::os::raw::c_char,
                &mut c_err,
            )
        };
        if res == ffi::TILEDB_OK && !c_key.is_null() && !c_val.is_null() {
            let (key, val) = unsafe {
                let k = String::from(
                    std::ffi::CStr::from_ptr(c_key).to_string_lossy(),
                );
                let v = String::from(
                    std::ffi::CStr::from_ptr(c_val).to_string_lossy(),
                );
                (k, v)
            };

            unsafe {
                // TODO: Ignoring the errors here since I have no idea how we'd
                // do anything abou them.
                ffi::tiledb_config_iter_next(*self.raw, &mut c_err);
            }

            Some((key, val))
        } else {
            None
        }
    }
}

/// Convenience for setting some of the more commonly-used configuration options.
#[derive(Clone, Debug)]
pub enum CommonOption {
    /// Sets an AES256GCM encryption key.
    Aes256GcmEncryptionKey(Vec<u8>),

    /// URL for REST server to use for remote arrays.
    RestServerAddress(String),

    /// Username for login to REST server.
    RestUsername(String),

    /// Password for login to REST server.
    RestPassword(String),

    /// Authentication token for REST server (used instead of username/password).
    RestToken(String),

    /// Have curl ignore ssl peer and host validation for REST server.
    RestIgnoreSslValidation(bool),
}

impl CommonOption {
    fn apply(&self, config: &mut Config) -> CApiResult<()> {
        match self {
            Self::Aes256GcmEncryptionKey(key) => {
                config.set("sm.encryption_type", "AES_256_GCM")?;
                config.set("sm.encryption_key", key)
            }
            Self::RestServerAddress(address) => {
                config.set("rest.server_address", address)
            }
            Self::RestUsername(username) => {
                config.set("rest.username", username)
            }
            Self::RestPassword(password) => {
                config.set("rest.password", password)
            }
            Self::RestToken(token) => config.set("rest.token", token),
            Self::RestIgnoreSslValidation(ignore_ssl_validation) => config.set(
                "rest.ignore_ssl_validation",
                if *ignore_ssl_validation {
                    "true"
                } else {
                    "false"
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn config_alloc() {
        Config::new().expect("Error creating config instance.");
    }

    #[test]
    fn config_set() {
        let mut cfg = Config::new().expect("Error creating config instance.");

        let val = cfg
            .get("rs.tiledb.test_key")
            .expect("Error getting config key.");
        assert!(val.is_none());

        cfg.set("rs.tiledb.test_key", "foobar")
            .expect("Error setting config key.");

        let val = cfg
            .get("rs.tiledb.test_key")
            .expect("Error getting config key.");
        assert_eq!(val.unwrap(), "foobar");
    }

    #[test]
    fn config_get() {
        let cfg = Config::new().expect("Error creating config instance.");
        let val = cfg
            .get("sm.encryption_type")
            .expect("Error getting encryptin type.");
        assert_eq!(val.unwrap(), "NO_ENCRYPTION");
    }

    #[test]
    fn config_save_load() {
        let mut cfg1 = Config::new().expect("Error creating config instance.");
        cfg1.set("rs.tiledb.test_key", "foobar")
            .expect("Error setting config key");
        cfg1.save("test.config")
            .expect("Error saving config to disk.");

        assert!(Path::new("test.config").exists());

        let mut cfg2 = Config::new().expect("Error creating config instance.");
        cfg2.load("test.config")
            .expect("Error loading config file.");
        let val = cfg2
            .get("rs.tiledb.test_key")
            .expect("Error getting config key.");
        assert_eq!(val.unwrap(), "foobar");
    }

    #[test]
    fn config_compare() {
        let cfg1 = Config::new().expect("Error creating config instance.");
        let mut cfg2 = Config::new().expect("Error creating config instance.");
        assert!(cfg1 == cfg2);

        cfg2.set("foo", "bar").expect("Error setting config key.");
        assert!(cfg1 != cfg2);
    }

    #[test]
    fn config_iter() {
        let cfg = Config::new().expect("Error creating config instance.");
        let mut count = 0;
        for (k, _) in &cfg {
            assert!(!k.is_empty());
            count += 1;
        }
        assert!(count > 0);

        let val = cfg
            .get("sm.encryption_type")
            .expect("Error getting config key.");
        assert_eq!(val.unwrap(), "NO_ENCRYPTION");
    }

    #[test]
    fn config_with_common_options() {
        let common_options = vec![
            CommonOption::Aes256GcmEncryptionKey("xyz".as_bytes().to_vec()),
            CommonOption::RestServerAddress(
                "http://localhost:8080".to_string(),
            ),
            CommonOption::RestUsername("foo".to_string()),
            CommonOption::RestPassword("bar".to_string()),
            CommonOption::RestToken("baz".to_string()),
            CommonOption::RestIgnoreSslValidation(true),
        ];

        let mut cfg = Config::new().expect("Error creating config instance.");
        for opt in common_options {
            cfg = cfg
                .with_common_option(&opt)
                .expect("Error setting common option.");
        }

        let key_to_val = [
            ("sm.encryption_type", "AES_256_GCM"),
            ("sm.encryption_key", "xyz"),
            ("rest.server_address", "http://localhost:8080"),
            ("rest.username", "foo"),
            ("rest.password", "bar"),
            ("rest.token", "baz"),
            ("rest.ignore_ssl_validation", "true"),
        ];

        for (key, val) in key_to_val {
            let result: Option<String> =
                cfg.get(key).expect("Error getting config key.");
            assert_eq!(result.unwrap(), val);

            cfg.set(key, "new").expect("Error setting config key.");

            let result: Option<String> =
                cfg.get(key).expect("Error getting config key.");
            assert_eq!(result.unwrap(), "new");
        }
    }
}
