use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use syn::parse::{Parse, Parser};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::Path;

pub fn expand(input: &syn::DeriveInput) -> TokenStream {
    query_capi_interface_impl(input).into()
}

fn query_capi_interface_impl(
    input: &syn::DeriveInput,
) -> proc_macro2::TokenStream {
    match input.data {
        syn::Data::Struct(ref structdata) => match structdata.fields {
            syn::Fields::Named(ref fields) => {
                match query_capi_interface_fields_named(input, fields) {
                    Some(tt) => tt,
                    None => quote_spanned! {
                        fields.span() => compile_error!("expected field with #[raw_query] or #[base(Query)] attribute")
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

fn query_capi_interface_fields_named(
    input: &syn::DeriveInput,
    fields: &syn::FieldsNamed,
) -> Option<proc_macro2::TokenStream> {
    for f in fields.named.iter() {
        for a in f.attrs.iter() {
            if let syn::AttrStyle::Outer = a.style {
                match a.meta {
                    syn::Meta::Path(_) => continue,
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
                            if mpath == vec!["Query"] {
                                return Some(
                                    query_capi_interface_impl_fields_named_base(
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

fn query_capi_interface_impl_fields_named_base(
    input: &syn::DeriveInput,
    f: &syn::Field,
) -> proc_macro2::TokenStream {
    let name = &input.ident;

    let fname =
        Ident::new(&f.ident.as_ref().unwrap().to_string(), Span::call_site());

    let impl_generics = input.generics.clone();
    let ty_generics = input.generics.clone();

    let where_clause = {
        let bound = {
            let field_type = crate::ty::try_deref(&f.ty);
            let parser = syn::WherePredicate::parse;
            parser.parse(quote!(#field_type: Query).into()).unwrap()
        };

        if input.generics.where_clause.is_some() {
            let mut wc = input.generics.where_clause.clone().unwrap();
            wc.predicates.push(bound);
            Some(wc)
        } else {
            Some(syn::WhereClause {
                where_token: Token![where](Span::call_site()),
                predicates: vec![bound].into_iter().collect(),
            })
        }
    };

    let expanded = quote! {
        impl #impl_generics Query for #name #ty_generics #where_clause {
            fn base(&self) -> &QueryBase {
                self.#fname.base()
            }

            fn finalize(self) -> TileDBResult<Array> {
                self.#fname.finalize()
            }
        }
    };

    expanded
}
