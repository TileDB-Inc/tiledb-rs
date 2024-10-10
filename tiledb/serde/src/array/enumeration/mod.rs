/// Encapsulation of data needed to construct an Enumeration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, OptionSubset)]
pub struct EnumerationData {
    pub name: String,
    pub datatype: Datatype,
    pub cell_val_num: Option<u32>,
    pub ordered: Option<bool>,
    pub data: Box<[u8]>,
    pub offsets: Option<Box<[u64]>>,
}

#[cfg(feature = "api-conversions")]
mod conversions;

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;
