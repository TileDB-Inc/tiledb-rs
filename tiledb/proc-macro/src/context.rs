use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use syn::parse::{Parse, Parser};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{GenericParam, Lifetime, LifetimeParam, Path};

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

    let fname =
        Ident::new(&f.ident.as_ref().unwrap().to_string(), Span::call_site());

    /*
     * If the struct has a <'ctx> bound already then we can re-use
     * the struct generics for the impl generics. Otherwise we have
     * to add <'ctx> to the impl generics.
     */
    let impl_generics = {
        let mut has_ctx_bound = false;
        for generic in input.generics.params.iter() {
            match generic {
                GenericParam::Lifetime(ref l) => {
                    if l.lifetime.ident.to_string() == "ctx" {
                        has_ctx_bound = true;
                        break;
                    }
                }
                _ => continue,
            }
        }

        if has_ctx_bound {
            input.generics.clone()
        } else {
            let mut g = input.generics.clone();
            g.params.insert(
                0,
                GenericParam::Lifetime(LifetimeParam::new(Lifetime::new(
                    "'ctx",
                    Span::call_site(),
                ))),
            );
            g
        }
    };

    let ty_generics = input.generics.clone();

    /*
     * It is perfectly fine to write the same trait bound multiple times,
     * or write trait bounds on non-generic types. For simplicity, always
     * add a ContextBound<'ctx> bound to the base field.
     */
    let where_clause = {
        let bound = {
            let field_type = &f.ty;
            let parser = syn::WherePredicate::parse;
            parser
                .parse(quote!(#field_type: ContextBound<'ctx>).into())
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

    let expanded = quote! {
        impl #impl_generics ContextBound<'ctx> for #name #ty_generics #where_clause {
            fn context(&self) -> &'ctx Context {
                self.#fname.context()
            }
        }
    };

    expanded.into()
}
