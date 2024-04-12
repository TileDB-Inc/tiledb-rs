use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::Path;

pub fn expand(input: &syn::DeriveInput) -> TokenStream {
    context_bound_impl(input).into()
}

fn context_bound_impl(input: &syn::DeriveInput) -> proc_macro2::TokenStream {
    match input.data {
        syn::Data::Struct(ref structdata) => match structdata.fields {
            syn::Fields::Named(ref fields) => {
                match context_bound_impl_fields_named(input, fields) {
                    Some(tt) => tt,
                    None => quote_spanned! {
                        fields.span() => compile_error!("expected field with #[context] or #[base(ContextBound)] attribute")
                    },
                }
            }
            syn::Fields::Unnamed(_) => {
                unimplemented!("syn::Fields::Unnamed")
            }
            syn::Fields::Unit => unimplemented!("syn::Fields::Unit"),
        },
        syn::Data::Enum(_) => unimplemented!("syn::Data::Enum"),
        syn::Data::Union(_) => unimplemented!("syn::Data::Union"),
    }
}

fn context_bound_impl_fields_named(
    input: &syn::DeriveInput,
    fields: &syn::FieldsNamed,
) -> Option<proc_macro2::TokenStream> {
    for f in fields.named.iter() {
        for a in f.attrs.iter() {
            if let syn::AttrStyle::Outer = a.style {
                match a.meta {
                    syn::Meta::Path(ref p) => {
                        let mpath = p
                            .segments
                            .iter()
                            .map(|p| p.ident.to_string())
                            .collect::<Vec<String>>();
                        if mpath == vec!["context"] {
                            return Some(
                                context_bound_impl_fields_named_direct(
                                    input, f,
                                ),
                            );
                        }
                    }
                    syn::Meta::List(ref ll) => {
                        let parser =
                            Punctuated::<Path, Token![,]>::parse_terminated;

                        let paths: Punctuated<Path, Token![,]> =
                            parser.parse(ll.tokens.clone().into())
                            .expect("Attribute #[base] argument is not a comma-delimited list of type paths");
                        for p in paths {
                            let mpath = p
                                .segments
                                .iter()
                                .map(|p| p.ident.to_string())
                                .collect::<Vec<String>>();
                            if mpath == vec!["ContextBound"]
                                || mpath == vec!["context", "ContextBound"]
                            {
                                return Some(
                                    context_bound_impl_fields_named_base(
                                        input, f,
                                    ),
                                );
                            }
                        }
                    }
                    syn::Meta::NameValue(_) => unimplemented!(),
                }
            }
        }
    }
    None
}

fn context_bound_impl_fields_named_direct(
    input: &syn::DeriveInput,
    f: &syn::Field,
) -> proc_macro2::TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) =
        input.generics.split_for_impl();
    let fname =
        Ident::new(&f.ident.as_ref().unwrap().to_string(), Span::call_site());
    let expanded = quote! {
        impl #impl_generics ContextBound <'ctx> for #name #ty_generics #where_clause {
            fn context(&self) -> &'ctx Context {
                self.#fname
            }
        }
    };
    expanded
}

fn context_bound_impl_fields_named_base(
    input: &syn::DeriveInput,
    f: &syn::Field,
) -> proc_macro2::TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) =
        input.generics.split_for_impl();
    let fname =
        Ident::new(&f.ident.as_ref().unwrap().to_string(), Span::call_site());
    let expanded = quote! {
        impl #impl_generics ContextBound<'ctx> for #name #ty_generics #where_clause {
            fn context(&self) -> &'ctx Context {
                self.#fname.context()
            }
        }
    };
    expanded
}
