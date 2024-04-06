use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use syn::spanned::Spanned;

pub fn expand(input: &syn::DeriveInput) -> TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) =
        input.generics.split_for_impl();
    let body = context_bound_body(input);

    let expanded = quote! {
        impl #impl_generics ContextBound <'ctx> for #name #ty_generics #where_clause {
            fn context(&self) -> &'ctx Context {
                #body
            }
        }
    };
    expanded.into()
}

fn context_bound_body(input: &syn::DeriveInput) -> proc_macro2::TokenStream {
    match input.data {
        syn::Data::Struct(ref structdata) => match structdata.fields {
            syn::Fields::Named(ref fields) => {
                match context_bound_body_fields_named(fields) {
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

fn context_bound_body_fields_named(
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
                            let fname = Ident::new(
                                &f.ident.as_ref().unwrap().to_string(),
                                Span::call_site(),
                            );
                            return Some(quote! {
                                self.#fname
                            });
                        } else if mpath == vec!["ContextBound"] {
                            let fname = Ident::new(
                                &f.ident.as_ref().unwrap().to_string(),
                                Span::call_site(),
                            );
                            return Some(quote! {
                                self.#fname.context()
                            });
                        }
                    }
                    syn::Meta::List(_) => {
                        unimplemented!()
                    }
                    syn::Meta::NameValue(_) => unimplemented!(),
                }
            }
        }
    }
    None
}
