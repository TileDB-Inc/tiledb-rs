extern crate trybuild;

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/array/*.rs");
}
