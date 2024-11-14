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
    /// Returns the number of variants of this enumeration.
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

    /// Returns the variants of this enumeration re-organized into a list of records.
    ///
    /// Each record is raw bytes. It is the user's responsibility to reinterpret these
    /// as physical values of [Self::datatype].
    ///
    /// If the enumeration's [CellValNum] is
    /// * [CellValNum::Fixed], then each of the inner [Vec]s will have the same length.
    /// * [CellValNum::Var], then the inner [Vec]s may each be of any size.
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
            let fixed = self.datatype.size()
                * u32::from(self.cell_val_num.unwrap_or(CellValNum::single()))
                    as usize;
            self.data
                .chunks(fixed)
                .map(|s| s.to_vec())
                .collect::<Vec<Vec<u8>>>()
        }
    }
}

/// Returns a (raw bytes, offsets) pair representing the input set of records
/// for the given [CellValNum].
///
/// This is the inverse of [EnumerationData::records].
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

    fn do_variants_records_integrity(enumeration: EnumerationData) {
        assert_eq!(0, enumeration.data.len() % enumeration.datatype.size());

        let records = enumeration.records();

        match enumeration.cell_val_num.unwrap_or(CellValNum::single()) {
            CellValNum::Fixed(nz) => {
                let byte_len = enumeration.datatype.size() * nz.get() as usize;
                for record in records {
                    assert_eq!(byte_len, record.len());
                }
            }
            CellValNum::Var => {
                let offsets = enumeration.offsets.as_ref().unwrap();
                assert_eq!(records.len(), offsets.len());

                let offsets = offsets
                    .iter()
                    .copied()
                    .chain(std::iter::once(enumeration.data.len() as u64))
                    .collect::<Vec<_>>();
                for (offsets, record) in offsets.windows(2).zip(records.iter())
                {
                    let (lb, ub) = (offsets[0] as usize, offsets[1] as usize);
                    assert_eq!(ub - lb, record.len());
                    assert_eq!(0, record.len() % enumeration.datatype.size());
                }
            }
        }
    }

    // NB: do not use Arbitrary because that *depends* on the roundtrip test
    fn strat_enumeration() -> impl Strategy<Value = EnumerationData> {
        (any::<Datatype>(), any::<CellValNum>()).prop_flat_map(
            move |(dt, cvn)| {
                super::strategy::prop_enumeration_for_datatype(
                    dt,
                    cvn,
                    &Default::default(),
                )
            },
        )
    }

    proptest! {
        #[test]
        fn variants_records_roundtrip(enumeration in strat_enumeration()) {
            do_variants_records_roundtrip(enumeration)
        }

        #[test]
        fn variants_records_integrity(enumeration in strat_enumeration()) {
            do_variants_records_integrity(enumeration)
        }
    }
}
