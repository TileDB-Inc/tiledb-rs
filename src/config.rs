extern crate tiledb_sys as ffi;

use crate::error::Error;

pub struct Config {
    _wrapped: *mut ffi::tiledb_config_t,
}

pub struct ConfigIterator<'a> {
    cfg: &'a Config,
    _wrapped: *mut ffi::tiledb_config_iter_t,
}

impl Config {
    pub fn new() -> Result<Config, String> {
        let mut cfg = Config {
            _wrapped: std::ptr::null_mut::<ffi::tiledb_config_t>(),
        };
        let mut err = Error::default();
        let res = unsafe {
            ffi::tiledb_config_alloc(
                &mut cfg._wrapped as *mut *mut ffi::tiledb_config_t,
                err.as_mut_ptr_ptr(),
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(cfg)
        } else {
            Err(err.get_message())
        }
    }

    pub fn set(&mut self, key: &str, val: &str) -> Result<(), String> {
        let c_key =
            std::ffi::CString::new(key).expect("Error creating CString");
        let c_val =
            std::ffi::CString::new(val).expect("Error creating CString");
        let mut err = Error::default();
        let res = unsafe {
            ffi::tiledb_config_set(
                self._wrapped,
                c_key.as_c_str().as_ptr(),
                c_val.as_c_str().as_ptr(),
                err.as_mut_ptr_ptr(),
            )
        };

        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(err.get_message())
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, String> {
        let c_key =
            std::ffi::CString::new(key).expect("Error creating CString");
        let mut val = std::ptr::null::<std::os::raw::c_char>();
        let mut err = Error::default();
        let res = unsafe {
            ffi::tiledb_config_get(
                self._wrapped,
                c_key.as_c_str().as_ptr(),
                &mut val as *mut *const std::os::raw::c_char,
                err.as_mut_ptr_ptr(),
            )
        };
        if res == ffi::TILEDB_OK && !val.is_null() {
            let c_msg = unsafe { std::ffi::CStr::from_ptr(val) };
            Ok(Some(String::from(c_msg.to_string_lossy())))
        } else if res == ffi::TILEDB_OK {
            Ok(None)
        } else {
            Err(err.get_message())
        }
    }

    pub fn unset(&mut self, key: &str) -> Result<(), String> {
        let c_key =
            std::ffi::CString::new(key).expect("Error creating CString");
        let mut err = Error::default();
        let res = unsafe {
            ffi::tiledb_config_unset(
                self._wrapped,
                c_key.as_c_str().as_ptr(),
                err.as_mut_ptr_ptr(),
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(err.get_message())
        }
    }

    pub fn load(&mut self, path: &str) -> Result<(), String> {
        let c_path =
            std::ffi::CString::new(path).expect("Error creating CString");
        let mut err = Error::default();
        let res = unsafe {
            ffi::tiledb_config_load_from_file(
                self._wrapped,
                c_path.as_c_str().as_ptr(),
                err.as_mut_ptr_ptr(),
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(err.get_message())
        }
    }

    pub fn save(&mut self, path: &str) -> Result<(), String> {
        let c_path =
            std::ffi::CString::new(path).expect("Error creating CString");
        let mut err = Error::default();
        let res = unsafe {
            ffi::tiledb_config_save_to_file(
                self._wrapped,
                c_path.as_c_str().as_ptr(),
                err.as_mut_ptr_ptr(),
            )
        };
        if res == ffi::TILEDB_OK {
            Ok(())
        } else {
            Err(err.get_message())
        }
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        if self._wrapped.is_null() {
            return;
        }
        unsafe {
            ffi::tiledb_config_free(
                &mut self._wrapped as *mut *mut ffi::tiledb_config_t,
            )
        }
    }
}

impl PartialEq for Config {
    fn eq(&self, other: &Self) -> bool {
        let mut eq: u8 = 0;
        let res = unsafe {
            ffi::tiledb_config_compare(self._wrapped, other._wrapped, &mut eq)
        };
        if res == ffi::TILEDB_OK {
            eq == 1
        } else {
            false
        }
    }
}

impl<'a> IntoIterator for &'a Config {
    type Item = (String, String);
    type IntoIter = ConfigIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let mut iter = ConfigIterator {
            cfg: self,
            _wrapped: std::ptr::null_mut::<ffi::tiledb_config_iter_t>(),
        };
        let c_path = std::ptr::null::<std::os::raw::c_char>();
        let mut err = Error::default();
        let res = unsafe {
            ffi::tiledb_config_iter_alloc(
                iter.cfg._wrapped,
                c_path,
                &mut iter._wrapped,
                err.as_mut_ptr_ptr(),
            )
        };

        if res == ffi::TILEDB_OK {
            iter
        } else {
            panic!("Not entirely sure what to do here.")
        }
    }
}

impl<'a> Drop for ConfigIterator<'a> {
    fn drop(&mut self) {
        if self._wrapped.is_null() {
            return;
        }
        unsafe {
            ffi::tiledb_config_iter_free(
                &mut self._wrapped as *mut *mut ffi::tiledb_config_iter_t,
            )
        }
    }
}

impl<'a> Iterator for ConfigIterator<'a> {
    type Item = (String, String);
    fn next(&mut self) -> Option<Self::Item> {
        let mut c_key = std::ptr::null::<std::os::raw::c_char>();
        let mut c_val = std::ptr::null::<std::os::raw::c_char>();
        let mut err = Error::default();
        let mut done: i32 = 0;
        let res = unsafe {
            ffi::tiledb_config_iter_done(
                self._wrapped,
                &mut done,
                err.as_mut_ptr_ptr(),
            )
        };

        if res != ffi::TILEDB_OK || done != 0 {
            return None;
        }

        let res = unsafe {
            ffi::tiledb_config_iter_here(
                self._wrapped,
                &mut c_key as *mut *const std::os::raw::c_char,
                &mut c_val as *mut *const std::os::raw::c_char,
                err.as_mut_ptr_ptr(),
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

            let mut next_err = Error::default();
            unsafe {
                // TODO: Ignoring the errors here since I have no idea how we'd
                // do anything abou them.
                ffi::tiledb_config_iter_next(
                    self._wrapped,
                    next_err.as_mut_ptr_ptr(),
                );
            }

            Some((key, val))
        } else {
            None
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
}
