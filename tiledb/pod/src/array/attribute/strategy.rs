use std::rc::Rc;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use strategy_ext::StrategyExt;
use tiledb_common::array::{ArrayType, CellValNum};
use tiledb_common::datatype::physical::strategy::PhysicalValueStrategy;
use tiledb_common::datatype::Datatype;
use tiledb_common::filter::FilterData;
use tiledb_common::physical_type_go;

use crate::array::attribute::{AttributeData, FillData};
use crate::array::domain::DomainData;
use crate::filter::strategy::{
    FilterPipelineStrategy, FilterPipelineValueTree,
    Requirements as FilterRequirements,
};

impl AttributeData {
    /// Returns a strategy for generating values of this attribute's type.
    pub fn value_strategy(&self) -> PhysicalValueStrategy {
        use proptest::prelude::*;
        use tiledb_common::filter::{
            CompressionData, CompressionType, FilterData,
        };
        use tiledb_common::physical_type_go;

        let has_double_delta = self.filters.iter().any(|f| {
            matches!(
                f,
                FilterData::Compression(CompressionData {
                    kind: CompressionType::DoubleDelta { .. },
                    ..
                })
            )
        });

        physical_type_go!(self.datatype, DT, {
            if has_double_delta {
                if std::any::TypeId::of::<DT>() == std::any::TypeId::of::<u64>()
                {
                    // see core `DoubleDelta::compute_bitsize`
                    let min = 0u64;
                    let max = u64::MAX >> 1;
                    return PhysicalValueStrategy::from((min..=max).boxed());
                } else if std::any::TypeId::of::<DT>()
                    == std::any::TypeId::of::<i64>()
                {
                    let min = i64::MIN >> 2;
                    let max = i64::MAX >> 2;
                    return PhysicalValueStrategy::from((min..=max).boxed());
                }
            }
            PhysicalValueStrategy::from(any::<DT>().boxed())
        })
    }
}

#[derive(Clone)]
pub enum StrategyContext {
    /// This attribute is being generated for an array schema
    Schema(ArrayType, Rc<DomainData>),
}

#[derive(Clone, Default)]
pub struct Requirements {
    pub name: Option<String>,
    pub datatype: Option<Datatype>,
    pub nullability: Option<bool>,
    pub context: Option<StrategyContext>,
    pub filters: Option<Rc<FilterRequirements>>,
}

pub fn prop_attribute_name() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-zA-Z0-9_]*")
        .expect("Error creating attribute name strategy")
        .prop_filter(
            "Attribute names may not begin with reserved prefix",
            |name| !name.starts_with("__"),
        )
}

fn prop_fill<T: Arbitrary>(
    cell_val_num: CellValNum,
) -> impl Strategy<Value = Vec<T>> {
    match cell_val_num {
        CellValNum::Fixed(nz) => {
            proptest::collection::vec(any::<T>(), nz.get() as usize)
        }
        CellValNum::Var => {
            proptest::collection::vec(any::<T>(), 1..16) /* TODO: does 16 make sense? */
        }
    }
}

/// Returns a strategy to generate a filter pipeline for an attribute with the given
/// datatype and other user requirements
fn prop_filters(
    datatype: Datatype,
    cell_val_num: CellValNum,
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = Vec<FilterData>> {
    use crate::filter::strategy::StrategyContext as FilterContext;

    let pipeline_requirements = FilterRequirements {
        context: Some(
            if let Some(StrategyContext::Schema(array_type, domain)) =
                requirements.context.as_ref()
            {
                FilterContext::SchemaAttribute(
                    datatype,
                    cell_val_num,
                    *array_type,
                    domain.clone(),
                )
            } else {
                FilterContext::Attribute(datatype, cell_val_num)
            },
        ),
        input_datatype: Some(datatype),
        ..requirements
            .filters
            .as_ref()
            .map(|rc| rc.as_ref().clone())
            .unwrap_or_default()
    };

    FilterPipelineStrategy::new(Rc::new(pipeline_requirements))
}

/// Returns a strategy for generating an arbitrary Attribute of the given datatype
/// that satisfies the other user requirements
fn prop_attribute_for_datatype(
    datatype: Datatype,
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = AttributeData> {
    physical_type_go!(
        datatype,
        DT,
        {
            let name = requirements
                .name
                .as_ref()
                .map(|n| Just(n.clone()).boxed())
                .unwrap_or(prop_attribute_name().boxed());
            let nullable = requirements
                .nullability
                .as_ref()
                .map(|n| Just(*n).boxed())
                .unwrap_or(any::<bool>().boxed());
            let cell_val_num = if datatype == Datatype::Any {
                Just(CellValNum::Var).boxed()
            } else {
                any::<CellValNum>()
            };
            let fill_nullable = any::<bool>();
            (name, nullable, cell_val_num, fill_nullable).prop_flat_map(
                move |(name, nullable, cell_val_num, fill_nullable)| {
                    (
                        prop_fill::<DT>(cell_val_num),
                        prop_filters(
                            datatype,
                            cell_val_num,
                            requirements.clone(),
                        ),
                    )
                        .prop_map(
                            move |(fill, filters)| AttributeData {
                                name: name.clone(),
                                datatype,
                                nullability: Some(nullable),
                                cell_val_num: Some(cell_val_num),
                                fill: Some(FillData {
                                    data: fill.into(),
                                    nullability: Some(
                                        nullable && fill_nullable,
                                    ),
                                }),
                                filters,
                            },
                        )
                },
            )
        }
        .boxed()
    )
}

pub fn prop_attribute(
    requirements: Rc<Requirements>,
) -> impl Strategy<Value = AttributeData> {
    let datatype = requirements
        .datatype
        .map(|d| Just(d).boxed())
        .unwrap_or(any::<Datatype>());

    datatype
        .prop_flat_map(move |datatype| {
            prop_attribute_for_datatype(datatype, requirements.clone())
        })
        .value_tree_map(|vt| AttributeValueTree::new(vt.current()))
        .boxed()
}

impl Arbitrary for AttributeData {
    type Parameters = Option<Rc<Requirements>>;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        prop_attribute(params.unwrap_or_default()).boxed()
    }
}

#[derive(Clone, Debug)]
pub struct AttributeValueTree {
    name: String,
    datatype: Datatype,
    nullability: Option<bool>,
    cell_val_num: Just<Option<CellValNum>>, // TODO: enable shrinking, will help identify if Var is necessary for example
    fill: Just<Option<FillData>>,           // TODO: enable shrinking
    filters: FilterPipelineValueTree,
}

impl AttributeValueTree {
    pub fn new(attr: AttributeData) -> Self {
        Self {
            name: attr.name,
            datatype: attr.datatype,
            nullability: attr.nullability,
            cell_val_num: Just(attr.cell_val_num),
            fill: Just(attr.fill),
            filters: FilterPipelineValueTree::new(attr.filters),
        }
    }
}

impl ValueTree for AttributeValueTree {
    type Value = AttributeData;

    fn current(&self) -> Self::Value {
        AttributeData {
            name: self.name.clone(),
            datatype: self.datatype,
            nullability: self.nullability,
            cell_val_num: self.cell_val_num.current(),
            fill: self.fill.current(),
            filters: self.filters.current(),
        }
    }

    fn simplify(&mut self) -> bool {
        self.filters.simplify()
    }

    fn complicate(&mut self) -> bool {
        self.filters.complicate()
    }
}
