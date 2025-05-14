use tiledb_common::array::{CellOrder, TileOrder};

use crate::error::TryFromFFIError;

#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    #[derive(Debug)]
    pub enum TileOrder {
        RowMajor,
        ColumnMajor,
    }

    #[derive(Debug)]
    pub enum CellOrder {
        Unordered,
        RowMajor,
        ColumnMajor,
        Global,
        Hilbert,
    }
}

pub use ffi::CellOrder as FFICellOrder;
pub use ffi::TileOrder as FFITileOrder;

impl From<TileOrder> for FFITileOrder {
    fn from(order: TileOrder) -> FFITileOrder {
        match order {
            TileOrder::RowMajor => FFITileOrder::RowMajor,
            TileOrder::ColumnMajor => FFITileOrder::ColumnMajor,
        }
    }
}

impl TryFrom<FFITileOrder> for TileOrder {
    type Error = TryFromFFIError;

    fn try_from(order: FFITileOrder) -> Result<Self, Self::Error> {
        let order = match order {
            FFITileOrder::RowMajor => TileOrder::RowMajor,
            FFITileOrder::ColumnMajor => TileOrder::ColumnMajor,
            _ => return Err(TryFromFFIError::from_tile_order(order)),
        };
        Ok(order)
    }
}

impl From<CellOrder> for FFICellOrder {
    fn from(order: CellOrder) -> FFICellOrder {
        match order {
            CellOrder::Unordered => FFICellOrder::Unordered,
            CellOrder::RowMajor => FFICellOrder::RowMajor,
            CellOrder::ColumnMajor => FFICellOrder::ColumnMajor,
            CellOrder::Global => FFICellOrder::Global,
            CellOrder::Hilbert => FFICellOrder::Hilbert,
        }
    }
}

impl TryFrom<FFICellOrder> for CellOrder {
    type Error = TryFromFFIError;

    fn try_from(order: FFICellOrder) -> Result<Self, Self::Error> {
        let order = match order {
            FFICellOrder::Unordered => CellOrder::Unordered,
            FFICellOrder::RowMajor => CellOrder::RowMajor,
            FFICellOrder::ColumnMajor => CellOrder::ColumnMajor,
            FFICellOrder::Global => CellOrder::Global,
            FFICellOrder::Hilbert => CellOrder::Hilbert,
            _ => return Err(TryFromFFIError::from_cell_order(order)),
        };
        Ok(order)
    }
}
