#[cxx::bridge]
mod ffi {
    extern "Rust" {
        unsafe fn vec_u8_resize(v: &mut Vec<u8>, new_len: usize, value: u8);
    }
}

unsafe fn vec_u8_resize(v: &mut Vec<u8>, new_len: usize, value: u8) {
    v.resize(new_len, value)
}
