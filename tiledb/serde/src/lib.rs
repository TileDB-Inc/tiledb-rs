pub mod array;
pub mod filter;

#[cfg(feature = "api-conversions")]
pub trait Factory {
    type Item;

    fn create(&self, context: &context::Context) -> Result<Self::Item>;
}

#[cfg(any(test, feature = "proptest-strategies"))]
pub mod strategy;
