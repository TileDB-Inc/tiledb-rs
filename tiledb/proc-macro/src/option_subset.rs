use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};

pub fn expand(input: &syn::DeriveInput) -> TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) =
        input.generics.split_for_impl();
    let body = option_subset_body(input);

    let expanded = quote! {
        impl #impl_generics OptionSubset for #name #ty_generics #where_clause {
            fn option_subset(&self, other: &Self) -> bool {
                #body
            }
        }
    };
    expanded.into()
}

fn option_subset_fields(fields: &syn::Fields) -> proc_macro2::TokenStream {
    match fields {
        syn::Fields::Named(ref fields) => {
            let fieldcmp = fields.named.iter().map(|f| {
                let fname = f.ident.as_ref().unwrap();
                quote! {
                    if !self.#fname.option_subset(&other.#fname) {
                        return false;
                    }
                }
            });
            quote! {
                #(#fieldcmp)*
                true
            }
        }
        syn::Fields::Unnamed(ref fields) => {
            let fieldcmp = (0..fields.unnamed.len()).map(|idx| {
                let idx = syn::Index::from(idx);
                quote! {
                    if !self.#idx.option_subset(&other.#idx) {
                        return false;
                    }
                }
            });
            quote! {
                #(#fieldcmp)*
                true
            }
        }
        syn::Fields::Unit => quote! {
            true
        },
    }
}

fn option_subset_body(input: &syn::DeriveInput) -> proc_macro2::TokenStream {
    match input.data {
        syn::Data::Struct(ref structdata) => {
            option_subset_fields(&structdata.fields)
        }
        syn::Data::Enum(ref e) => {
            let variants = e.variants.iter().map(|v| {
                let ename = &input.ident;
                let vname = &v.ident;
                match v.fields {
                    syn::Fields::Named(ref fields) => {
                        let fnames = fields.named.iter().map(|f| f.ident.as_ref().unwrap()).collect::<Vec<_>>();

                        let self_fnames = fnames.iter().map(|f| {
                            let fname = format!("self_{}", f);
                            Ident::new(&fname, Span::call_site())
                        }).collect::<Vec<_>>();

                        let other_fnames = fnames.iter().map(|f| {
                            let fname = format!("other_{}", f);
                            Ident::new(&fname, Span::call_site())
                        }).collect::<Vec<_>>();

                        quote! {
                            (#ename::#vname { #(#fnames: #self_fnames,)* }, #ename::#vname { #(#fnames: #other_fnames,)* }) => {
                                #(if !#self_fnames.option_subset(&#other_fnames) { return false; })*
                                true
                            }
                        }
                    },
                    syn::Fields::Unnamed(ref fields) => {
                        let self_fnames = (0.. fields.unnamed.len()).map(|idx| {
                            let fname = format!("self_{}", idx);
                            Ident::new(&fname, Span::call_site())
                        }).collect::<Vec<_>>();

                        let other_fnames = (0.. fields.unnamed.len()).map(|idx| {
                            let fname = format!("other_{}", idx);
                            Ident::new(&fname, Span::call_site())
                        }).collect::<Vec<_>>();

                        quote! {
                            ( #ename::#vname ( #(ref #self_fnames,)* ), #ename::#vname ( #(ref #other_fnames,)* )) => {
                                #(if !#self_fnames.option_subset(&#other_fnames) { return false; })*
                                true
                            }
                        }
                    },
                    syn::Fields::Unit => quote! {
                        ( #ename::#vname, #ename::#vname ) => true
                    }
                }
            });
            quote! {
                match (self, other) {
                    #(#variants,)*
                    _ => false
                }
            }
        }
        syn::Data::Union(_) => unimplemented!("syn::Data::Union"),
    }
}
