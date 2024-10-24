#![cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "windows"
)))]

pub fn configure_rustc() {
    panic!("This operating system is not supported.");
}

pub fn merge_libraries(build_dir: &std::path::Path) {
    panic!("This operating system is not supported.");
}
