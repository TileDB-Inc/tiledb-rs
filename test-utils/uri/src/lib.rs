mod tempdir;

pub use tempdir::TestDirectory;

use anyhow::Result;

pub trait TestArrayUri {
    fn base_dir(&self) -> Result<String>;
    fn with_paths(&self, paths: &[&str]) -> Result<String>;
    fn close(self) -> Result<()>;

    fn with_path(&self, path: &str) -> Result<String> {
        self.with_paths(&[path])
    }
}

pub fn get_uri_generator() -> Result<impl TestArrayUri> {
    // TODO: Eventually this will check an environment variable to decide
    // whether we should return a TestDirectory or a new struct called something
    // like TestRestServer to run our test suite against the cloud service.
    tempdir::TestDirectory::new()
}
