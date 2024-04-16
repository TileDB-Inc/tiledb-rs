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
                        fields.span() => compile_error!("expected field with #[raw_query] or #[base(QueryCAPIInterface)] attribute")
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
    let mut array_field: Option<&syn::Field> = None;
    let mut query_field: Option<&syn::Field> = None;

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
                        if mpath == vec!["raw_array"] {
                            array_field = Some(f);
                        } else if mpath == vec!["raw_query"] {
                            query_field = Some(f);
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
                            if mpath == vec!["QueryCAPIInterface"] {
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

            if array_field.is_some() && query_field.is_some() {
                return Some(query_capi_interface_impl_fields_named_direct(
                    input,
                    array_field.unwrap(),
                    query_field.unwrap(),
                ));
            }
        }
    }
    None
}

fn query_capi_interface_impl_fields_named_direct(
    input: &syn::DeriveInput,
    array_field: &syn::Field,
    query_field: &syn::Field,
) -> proc_macro2::TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) =
        input.generics.split_for_impl();
    let array_fname = Ident::new(
        &array_field.ident.as_ref().unwrap().to_string(),
        Span::call_site(),
    );
    let query_fname = Ident::new(
        &query_field.ident.as_ref().unwrap().to_string(),
        Span::call_site(),
    );
    let expanded = quote! {
        impl #impl_generics QueryCAPIInterface for #name #ty_generics #where_clause {
            fn carray(&self) -> &RawArray {
                &self.#array_fname
            }
            fn cquery(&self) -> &RawQuery {
                &self.#query_fname
            }
        }
    };
    expanded
}

fn query_capi_interface_impl_fields_named_base(
    input: &syn::DeriveInput,
    f: &syn::Field,
) -> proc_macro2::TokenStream {
    let name = &input.ident;

    let fname =
        Ident::new(&f.ident.as_ref().unwrap().to_string(), Span::call_site());

    /*
     * It is perfectly fine to write the same trait bound multiple times,
     * or write trait bounds on non-generic types. For simplicity, always
     * add a QueryCAPIInterface bound to the base field.
     */
    let where_clause = {
        let bound = {
            let field_type = &f.ty;
            let parser = syn::WherePredicate::parse;
            parser
                .parse(quote!(#field_type: QueryCAPIInterface).into())
                .unwrap()
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

    let (impl_generics, ty_generics, _) = input.generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics QueryCAPIInterface for #name #ty_generics #where_clause {
            fn carray(&self) -> &RawArray {
                QueryCAPIInterface::carray(&self.#fname)
            }
            fn cquery(&self) -> &RawQuery {
                QueryCAPIInterface::cquery(&self.#fname)
            }
        }
    };

    expanded
}
