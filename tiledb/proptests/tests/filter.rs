use proptest::prelude::*;

use tiledb::array::attribute::Builder as AttributeBuilder;
use tiledb::context::Context;
use tiledb::datatype::Datatype;
use tiledb::{Factory, Result as TileDBResult};

use tiledb_proptests::filter as pt_filter;

#[test]
fn any_filter_test() -> TileDBResult<()> {
    let cfg = ProptestConfig::with_cases(1000);
    let ctx = Context::new()?;

    proptest!(cfg, |(fdata in pt_filter::prop_filter())| {
        let _ = fdata.create(&ctx)?;
    });

    Ok(())
}

#[test]
fn filter_list_test() -> TileDBResult<()> {
    let cfg = ProptestConfig::with_cases(10000);
    let ctx = Context::new()?;

    proptest!(cfg,
        |((datatype, cell_val_num, flistdata)
            in pt_filter::list::prop_any_filter_list(8))| {
        let flist = flistdata.create(&ctx)?;
        let attr = AttributeBuilder::new(&ctx, "attr_name", datatype)?;

        let attr = if !matches!(datatype, Datatype::Any) {
            attr.cell_val_num(cell_val_num)?
        } else {
            attr
        };

        let _ = attr
            .filter_list(flist)?
            .build();
    });

    Ok(())
}
