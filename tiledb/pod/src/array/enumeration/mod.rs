#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;

#[cfg(feature = "option-subset")]
use tiledb_utils::option::OptionSubset;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use tiledb_common::array::CellValNum;
use tiledb_common::datatype::Datatype;

/// Encapsulation of data needed to construct an Enumeration
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "option-subset", derive(OptionSubset))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct EnumerationData {
    pub name: String,
    pub datatype: Datatype,
    pub cell_val_num: Option<CellValNum>,
    pub ordered: Option<bool>,
    pub data: Box<[u8]>,
    pub offsets: Option<Box<[u64]>>,
}

impl EnumerationData {
    pub fn num_variants(&self) -> usize {
        if let Some(offsets) = self.offsets.as_ref() {
            offsets.len()
        } else {
            let fixed_cvn =
                u32::from(self.cell_val_num.unwrap_or(CellValNum::single()));
            let fixed_cvn = usize::try_from(fixed_cvn).unwrap();

            self.data.len() / self.datatype.size() / fixed_cvn
        }
    }

    pub fn records(&self) -> Vec<Vec<u8>> {
        if let Some(offsets) = self.offsets.as_ref() {
            let last_window =
                [*offsets.last().unwrap(), self.data.len() as u64];
            offsets
                .windows(2)
                .chain(std::iter::once(last_window.as_ref()))
                .map(|w| self.data[w[0] as usize..w[1] as usize].to_vec())
                .collect::<Vec<Vec<u8>>>()
        } else {
            let fixed =
                u32::from(self.cell_val_num.unwrap_or(CellValNum::single()));
            self.data
                .chunks(fixed as usize)
                .map(|s| s.to_vec())
                .collect::<Vec<Vec<u8>>>()
        }
    }
}

pub fn variants_from_records(
    cell_val_num: CellValNum,
    variants: Vec<Vec<u8>>,
) -> (Box<[u8]>, Option<Box<[u64]>>) {
    let (data, offsets) = match cell_val_num {
        CellValNum::Fixed(_) => {
            (variants.into_iter().flatten().collect::<Vec<u8>>(), None)
        }
        CellValNum::Var => {
            let offsets = {
                let mut offsets = vec![0];
                variants.iter().take(variants.len() - 1).for_each(|v| {
                    offsets.push(offsets.last().unwrap() + v.len())
                });
                offsets.into_iter().map(|o| o as u64).collect::<Vec<u64>>()
            };
            let data = variants.into_iter().flatten().collect::<Vec<u8>>();
            (data, Some(offsets))
        }
    };
    (
        data.into_boxed_slice(),
        offsets.map(|o| o.into_boxed_slice()),
    )
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn do_variants_records_roundtrip(e: EnumerationData) {
        let records = e.records();
        let (data_out, offsets_out) = variants_from_records(
            e.cell_val_num.unwrap_or(CellValNum::single()),
            records,
        );
        assert_eq!(e.data, data_out);
        assert_eq!(e.offsets, offsets_out);
    }

    // NB: do not use Arbitrary because that *depends* on the roundtrip test
    fn strat_variants_records_roundtrip(
    ) -> impl Strategy<Value = EnumerationData> {
        (any::<Datatype>(), any::<CellValNum>()).prop_flat_map(
            move |(dt, cvn)| {
                super::strategy::prop_enumeration_for_datatype(dt, cvn, 1, 4)
            },
        )
    }

    proptest! {
        #[test]
        fn variants_records_roundtrip(enumeration in strat_variants_records_roundtrip()) {
            do_variants_records_roundtrip(enumeration)
        }
    }
}
