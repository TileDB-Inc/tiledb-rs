// use std::rc::Rc;
//
// use proptest::prelude::*;
// use serde_json::json;
//
// use crate::array::{
//     attribute::FillData, ArrayType, AttributeData, CellValNum, DomainData,
// };
// use crate::datatype::strategy::*;
// use crate::filter::list::FilterListData;
// use crate::{fn_typed, Datatype};
//
// #[derive(Clone)]
// pub enum StrategyContext {
//     /// This attribute is being generated for an array schema
//     Schema(ArrayType, Rc<DomainData>),
// }
//
// #[derive(Clone, Default)]
// pub struct Requirements {
//     pub name: Option<String>,
//     pub datatype: Option<Datatype>,
//     pub nullability: Option<bool>,
//     pub context: Option<StrategyContext>,
// }
//
// pub fn prop_attribute_name() -> impl Strategy<Value = String> {
//     proptest::string::string_regex("[a-zA-Z0-9_]*")
//         .expect("Error creating attribute name strategy")
//         .prop_filter(
//             "Attribute names may not begin with reserved prefix",
//             |name| !name.starts_with("__"),
//         )
// }
//
// fn prop_cell_val_num() -> impl Strategy<Value = Option<CellValNum>> {
//     Just(None)
// }
//
// fn prop_fill<T: Arbitrary>() -> impl Strategy<Value = T> {
//     any::<T>()
// }
//
// /// Returns a strategy to generate a filter pipeline for an attribute with the given
// /// datatype and other user requirements
// fn prop_filters(
//     datatype: Datatype,
//     requirements: Rc<Requirements>,
// ) -> impl Strategy<Value = FilterListData> {
//     use crate::filter::strategy::{
//         Requirements as FilterRequirements, StrategyContext as FilterContext,
//     };
//
//     let pipeline_requirements = FilterRequirements {
//         context: requirements.context.as_ref().map(
//             |StrategyContext::Schema(array_type, domain)| {
//                 FilterContext::Attribute(datatype, *array_type, domain.clone())
//             },
//         ),
//         input_datatype: Some(datatype),
//     };
//
//     any_with::<FilterListData>(Rc::new(pipeline_requirements))
// }
//
// /// Returns a strategy for generating an arbitrary Attribute of the given datatype
// /// that satisfies the other user requirements
// fn prop_attribute_for_datatype(
//     datatype: Datatype,
//     requirements: Rc<Requirements>,
// ) -> impl Strategy<Value = AttributeData> {
//     fn_typed!(datatype, DT, {
//         let name = requirements
//             .name
//             .as_ref()
//             .map(|n| Just(n.clone()).boxed())
//             .unwrap_or(prop_attribute_name().boxed());
//         let nullable = requirements
//             .nullability
//             .as_ref()
//             .map(|n| Just(*n).boxed())
//             .unwrap_or(any::<bool>().boxed());
//         let cell_val_num = prop_cell_val_num();
//         let fill = prop_fill::<DT>();
//         let fill_nullable = any::<bool>();
//         let filters = prop_filters(datatype, requirements);
//         (name, nullable, cell_val_num, fill, fill_nullable, filters)
//             .prop_map(
//                 move |(
//                     name,
//                     nullable,
//                     cell_val_num,
//                     fill,
//                     fill_nullable,
//                     filters,
//                 )| {
//                     AttributeData {
//                         name,
//                         datatype,
//                         nullability: Some(nullable),
//                         cell_val_num,
//                         fill: Some(FillData {
//                             data: json!(fill),
//                             nullability: Some(nullable && fill_nullable),
//                         }),
//                         filters,
//                     }
//                 },
//             )
//             .boxed()
//     })
// }
//
// pub fn prop_attribute(
//     requirements: Rc<Requirements>,
// ) -> impl Strategy<Value = AttributeData> {
//     let datatype = requirements
//         .datatype
//         .map(|d| Just(d).boxed())
//         .unwrap_or(prop_datatype_implemented().boxed());
//
//     datatype.prop_flat_map(move |datatype| {
//         prop_attribute_for_datatype(datatype, requirements.clone())
//     })
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::{Context, Factory};
//     use util::assert_option_subset;
//     use util::option::OptionSubset;
//
//     /// Test that the arbitrary attribute construction always succeeds
//     #[test]
//     fn attribute_arbitrary() {
//         let ctx = Context::new().expect("Error creating context");
//
//         proptest!(|(attr in prop_attribute(Default::default()))| {
//             attr.create(&ctx).expect("Error constructing arbitrary attribute");
//         });
//     }
//
//     #[test]
//     fn attribute_eq_reflexivity() {
//         let ctx = Context::new().expect("Error creating context");
//
//         proptest!(|(attr in prop_attribute(Default::default()))| {
//             assert_eq!(attr, attr);
//             assert_option_subset!(attr, attr);
//
//             let attr = attr.create(&ctx)
//                 .expect("Error constructing arbitrary attribute");
//             assert_eq!(attr, attr);
//         });
//     }
// }
