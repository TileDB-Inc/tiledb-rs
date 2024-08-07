extern crate proc_macro;
extern crate proc_macro2;
#[macro_use]
extern crate quote;
#[macro_use]
extern crate syn;

use proc_macro::TokenStream;
use syn::DeriveInput;

mod option_subset;

#[proc_macro_derive(OptionSubset)]
pub fn derive_option_subset(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    option_subset::expand(&input)
}
