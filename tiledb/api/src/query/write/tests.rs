use super::*;
use crate::tests::examples::{TestArray, quickstart};
use crate::tests::prelude::*;

/// Test that we disallow attaching buffers
/// to fields with an incompatible physical type
#[test]
fn physical_type_compatibility() -> anyhow::Result<()> {
    let mut array = TestArray::new(
        "physical_type_compatibility",
        quickstart::Builder::new(ArrayType::Sparse).build().into(),
    )?;

    let rowdata_i64 = vec![1i64, 2i64, 3i64, 4i64];
    let rowdata_u32 = vec![1u32, 2u32, 3u32, 4u32];
    let rowdata_f32 = vec![1f32, 2f32, 3f32, 4f32];
    let rowdata_i16 = vec![1i16, 2i16, 3i16, 4i16];
    let rowdata_i32 = vec![1i32, 2i32, 3i32, 4i32];

    let w = WriteBuilder::new(array.for_write()?)?.data("rows", &rowdata_i64);
    assert!(matches!(
        w,
        Err(Error::Datatype(
            DatatypeError::PhysicalTypeIncompatible { .. }
        ))
    ));

    let w = WriteBuilder::new(array.for_write()?)?.data("rows", &rowdata_u32);
    assert!(matches!(
        w,
        Err(Error::Datatype(
            DatatypeError::PhysicalTypeIncompatible { .. }
        ))
    ));

    let w = WriteBuilder::new(array.for_write()?)?.data("rows", &rowdata_f32);
    assert!(matches!(
        w,
        Err(Error::Datatype(
            DatatypeError::PhysicalTypeIncompatible { .. }
        ))
    ));

    let w = WriteBuilder::new(array.for_write()?)?.data("rows", &rowdata_i16);
    assert!(matches!(
        w,
        Err(Error::Datatype(
            DatatypeError::PhysicalTypeIncompatible { .. }
        ))
    ));

    let w = WriteBuilder::new(array.for_write()?)?.data("rows", &rowdata_i32);
    assert!(w.is_ok());

    Ok(())
}

/// Test the above but for char, whose signed-ness is
/// platform-dependent
#[test]
fn physical_type_compatibility_char() -> anyhow::Result<()> {
    let mut array = TestArray::new("physical_type_compatibility_char", {
        let mut b = quickstart::Builder::new(ArrayType::Sparse);
        b.attribute().datatype = Datatype::Char;
        b.build().into()
    })?;

    let a_signed = vec![-20i8, 10i8, 0i8, 10i8, 20i8];
    let a_unsigned = vec![0u8, 50u8, 100u8, 150u8, 200u8];

    let w_signed = WriteBuilder::new(array.for_write()?)?.data("a", &a_signed);
    let w_unsigned =
        WriteBuilder::new(array.for_write()?)?.data("a", &a_unsigned);

    use std::any::TypeId;
    if TypeId::of::<std::ffi::c_char>() == TypeId::of::<u8>() {
        // unsigned
        assert!(matches!(
            w_signed,
            Err(Error::Datatype(
                DatatypeError::PhysicalTypeIncompatible { .. }
            ))
        ));
        assert!(w_unsigned.is_ok());
    } else {
        // signed
        assert!(w_signed.is_ok());
        assert!(matches!(
            w_unsigned,
            Err(Error::Datatype(
                DatatypeError::PhysicalTypeIncompatible { .. }
            ))
        ));
    }

    Ok(())
}
