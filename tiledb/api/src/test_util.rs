use tempfile::TempDir;

use crate::error::Error;
use crate::Result as TileDBResult;

pub trait TestArrayUri {
    fn base_dir(&self) -> TileDBResult<String>;
    fn with_path(&self, path: &str) -> TileDBResult<String>;
    fn with_paths(&self, paths: &[&str]) -> TileDBResult<String>;
    fn close(self) -> TileDBResult<()>;
}

pub fn get_uri_generator() -> TileDBResult<impl TestArrayUri> {
    // TODO: Eventually this will check an environment variable to decide
    // whether we should return a TestDirectory or a new struct called something
    // like TestRestServer to run our test suite against the cloud service.
    TestDirectory::new()
}

pub struct TestDirectory {
    base_dir: TempDir,
}

impl TestDirectory {
    pub fn new() -> TileDBResult<Self> {
        Ok(Self {
            base_dir: TempDir::new().map_err(|e| {
                Error::Other(format!(
                    "Error creating temporary directory: {}",
                    e
                ))
            })?,
        })
    }

    pub fn base_dir(&self) -> TileDBResult<String> {
        let path =
            self.base_dir.path().to_str().map(|s| s.to_string()).ok_or(
                Error::Other("Error creating test array URI".to_string()),
            )?;
        Ok("file://".to_string() + &path)
    }

    pub fn with_path(&self, path: &str) -> TileDBResult<String> {
        self.with_paths(&[path])
    }

    pub fn with_paths(&self, paths: &[&str]) -> TileDBResult<String> {
        let path = self.base_dir.path().to_path_buf();
        let path = paths.iter().fold(path, |path, part| path.join(part));
        let path = path
            .to_str()
            .map(|p| p.to_string())
            .ok_or(Error::Other("Error creating temporary URI".to_string()))?;
        Ok("file://".to_string() + &path)
    }

    pub fn close(self) -> TileDBResult<()> {
        self.base_dir.close().map_err(|e| {
            Error::Other(format!("Error closing temporary directory: {}", e))
        })
    }
}

impl TestArrayUri for TestDirectory {
    fn base_dir(&self) -> TileDBResult<String> {
        self.base_dir()
    }

    fn with_path(&self, path: &str) -> TileDBResult<String> {
        self.with_path(path)
    }

    fn with_paths(&self, paths: &[&str]) -> TileDBResult<String> {
        self.with_paths(paths)
    }

    fn close(self) -> TileDBResult<()> {
        self.close()
    }
}
