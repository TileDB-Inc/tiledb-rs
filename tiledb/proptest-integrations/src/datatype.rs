use super::*;
use crate::array::dimension::DimensionConstraints;
use crate::array::{
    ArrayType, AttributeBuilder, DimensionBuilder, DomainBuilder, Schema,
    SchemaBuilder,
};
use crate::error::Error;
use crate::{Context, Result as TileDBResult};

/// Creates a schema with a single dimension of the given `Datatype` with one attribute.
/// Used by the test to check if the `Datatype` can be used in this way.
fn dimension_comprehensive_schema(
    context: &Context,
    array_type: ArrayType,
    datatype: Datatype,
) -> TileDBResult<Schema> {
    let dim = physical_type_go!(datatype, DT, {
        if matches!(datatype, Datatype::StringAscii) {
            DimensionBuilder::new(
                context,
                "d",
                datatype,
                DimensionConstraints::StringAscii,
            )
        } else {
            let domain: [DT; 2] = [0 as DT, 127 as DT];
            let extent: DT = 16 as DT;
            DimensionBuilder::new(context, "d", datatype, (domain, extent))
        }
    })?
    .build();

    let attr = AttributeBuilder::new(context, "a", Datatype::Any)?.build();

    let domain = DomainBuilder::new(context)?.add_dimension(dim)?.build();
    SchemaBuilder::new(context, array_type, domain)?
        .add_attribute(attr)?
        .build()
}

fn do_dense_dimension_comprehensive(datatype: Datatype) {
    let allowed = DENSE_DIMENSION_DATATYPES.contains(&datatype);
    assert_eq!(allowed, datatype.is_allowed_dimension_type_dense());

    let context = Context::new().unwrap();
    let r =
        dimension_comprehensive_schema(&context, ArrayType::Dense, datatype);
    assert_eq!(allowed, r.is_ok(), "try_construct => {:?}", r.err());
    if let Err(Error::LibTileDB(s)) = r {
        assert!(
            s.contains("not a valid Dimension Datatype")
                || s.contains("do not support dimension datatype"),
            "Expected dimension datatype error, received: {}",
            s
        );
    } else {
        assert!(
            r.is_ok(),
            "Found error other than LibTileDB: {}",
            r.err().unwrap()
        );
    }
}

fn do_sparse_dimension_comprehensive(datatype: Datatype) {
    let allowed = SPARSE_DIMENSION_DATATYPES.contains(&datatype);
    assert_eq!(allowed, datatype.is_allowed_dimension_type_sparse());

    let context = Context::new().unwrap();
    let r =
        dimension_comprehensive_schema(&context, ArrayType::Sparse, datatype);
    assert_eq!(allowed, r.is_ok(), "try_construct => {:?}", r.err());
    if let Err(Error::LibTileDB(s)) = r {
        assert!(
            s.contains("not a valid Dimension Datatype")
                || s.contains("do not support dimension datatype"),
            "Expected dimension datatype error, received: {}",
            s
        );
    } else {
        assert!(
            r.is_ok(),
            "Found error other than LibTileDB: {}",
            r.err().unwrap()
        );
    }
}

proptest! {
    #[test]
    fn dense_dimension_comprehensive(dt in any::<Datatype>()) {
        do_dense_dimension_comprehensive(dt)
    }

    #[test]
    fn sparse_dimension_comprehensive(dt in any::<Datatype>()) {
        do_sparse_dimension_comprehensive(dt)
    }
}
