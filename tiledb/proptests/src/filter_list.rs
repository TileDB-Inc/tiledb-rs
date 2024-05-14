use proptest::prelude::*;
use proptest::strategy::{NewTree, Strategy, ValueTree};
use proptest::test_runner::{TestRng, TestRunner};

use rand::distributions::Uniform;
use rand::Rng;

use tiledb::array::schema::CellValNum;
use tiledb::datatype::Datatype;
use tiledb::filter::list::FilterListData;
use tiledb::filter::FilterData;

use crate::datatype;
use crate::filter;
use crate::util;

pub struct FilterListDataValueTree {
    filters: Vec<FilterData>,
    low: usize,
    current: usize,
    high: usize,
}

impl FilterListDataValueTree {
    pub fn new(filters: Vec<FilterData>) -> Self {
        let len = filters.len();
        Self {
            filters,
            low: 0,
            current: len,
            high: len,
        }
    }
}

impl ValueTree for FilterListDataValueTree {
    type Value = FilterListData;

    fn current(&self) -> Self::Value {
        FilterListData::from_iter(self.filters[0..self.current].iter().cloned())
    }

    fn simplify(&mut self) -> bool {
        let prev_current = self.current;
        assert!(self.low <= self.current && self.current <= self.high);
        self.high = self.current;
        self.current = ((self.high - self.low) / 2).clamp(self.low, self.high);
        self.current != prev_current
    }

    fn complicate(&mut self) -> bool {
        let prev_current = self.current;
        assert!(self.low <= self.current && self.current <= self.high);
        self.low = (self.current + 1).clamp(self.current, self.high);
        self.current = ((self.high - self.low) / 2).clamp(self.low, self.high);
        self.current != prev_current
    }
}

#[derive(Debug)]
pub struct FilterListDataStrategy {
    datatype: Datatype,
    cell_val_num: CellValNum,
    len_dist: Uniform<usize>,
}

impl FilterListDataStrategy {
    pub fn new(
        datatype: Datatype,
        cell_val_num: CellValNum,
        max_len: usize,
    ) -> Self {
        Self {
            datatype,
            cell_val_num,
            len_dist: Uniform::new_inclusive(0, max_len),
        }
    }
}

impl Strategy for FilterListDataStrategy {
    type Tree = FilterListDataValueTree;
    type Value = FilterListData;
    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        let len = runner.rng().sample(self.len_dist);
        let mut curr_type = self.datatype;
        let mut filters = Vec::new();
        for idx in 0..len {
            let fdata = filter::generate_with_constraints(
                runner,
                curr_type,
                self.cell_val_num,
                idx,
            );
            let next_type = fdata.transform_datatype(&curr_type);
            if next_type.is_none() {
                return Err(format!(
                    "INVALID FILTER DATA: {} {} {:?}",
                    curr_type, self.cell_val_num, fdata
                )
                .into());
            }
            filters.push(fdata);
            curr_type = next_type.unwrap();
        }

        Ok(FilterListDataValueTree::new(filters))
    }
}

pub fn prop_filter_list(
    datatype: Datatype,
    cell_val_num: CellValNum,
    max_len: usize,
) -> impl Strategy<Value = FilterListData> {
    FilterListDataStrategy::new(datatype, cell_val_num, max_len)
}

pub fn prop_any_filter_list(
    max_len: usize,
) -> impl Strategy<Value = (Datatype, CellValNum, FilterListData)> {
    let datatype = datatype::prop_all_datatypes();
    let cell_val_num = util::prop_cell_val_num();
    (datatype, cell_val_num).prop_flat_map(move |(datatype, cell_val_num)| {
        (
            Just(datatype),
            Just(cell_val_num),
            prop_filter_list(datatype, cell_val_num, max_len),
        )
    })
}

pub fn generate(
    rng: &mut TestRng,
    datatype: Datatype,
    cell_val_num: CellValNum,
) -> FilterListData {
    let num_filters = rng.gen_range(0..=8);
    let mut filters = Vec::new();
    let mut curr_type = datatype;
    for idx in 0..num_filters {
        let fdata = filter::generate_with_constraints(
            rng,
            curr_type,
            cell_val_num,
            idx,
        );
        let next_type = fdata.transform_datatype(&curr_type);
        if next_type.is_none() {
            return Err(format!(
                "INVALID FILTER DATA: {} {} {:?}",
                curr_type, cell_val_num, fdata
            )
            .into());
        }
        filters.push(fdata);
        curr_type = next_type.unwrap();
    }

    Ok(FilterListDataValueTree::new(filters))
}

pub fn gen_for_dimension(
    rng: &mut TestRng,
    dim: &DimensionData,
) -> FilterListData {
    generate(rng, dim.datatype, dim.cell_val_num.unwrap())
}

pub fn gen_for_attribute(
    rng: &mut TestRng,
    attr: &AttributeData,
) -> FilterListData {
    generate(rng, attr.datatype, attr.cell_val_num.unwrap())
}
