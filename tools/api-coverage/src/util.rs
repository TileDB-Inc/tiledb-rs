use std::fs;
use std::io::Read;

use anyhow::Result;
use quote::ToTokens;
use walkdir::WalkDir;

pub fn parse_file(path: &str) -> Result<syn::File> {
    let mut file = fs::File::open(path)?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    let ast = syn::parse_file(&content)?;
    Ok(ast)
}

pub fn walk_rust_sources<F>(path: &str, mut callback: F)
where
    F: FnMut(String),
{
    for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
        if entry.path().extension().is_none_or(|ext| ext != "rs") {
            continue;
        }
        callback(entry.path().display().to_string());
    }
}

pub fn unparse_constant(constant: &syn::ItemConst) -> String {
    let item = syn::Item::Const(constant.clone());
    let file = syn::File {
        shebang: None,
        attrs: vec![],
        items: vec![item],
    };
    prettyplease::unparse(&file).trim().to_owned()
}

pub fn unparse_signature(sig: &syn::Signature) -> String {
    let tokens = sig.to_token_stream();
    let group = proc_macro2::Group::new(proc_macro2::Delimiter::Brace, tokens);
    let delimspan = group.delim_span();

    let foreignitemfn = syn::ForeignItemFn {
        attrs: vec![],
        vis: syn::Visibility::Inherited,
        sig: sig.clone(),
        semi_token: syn::token::Semi {
            spans: [proc_macro2::Span::call_site()],
        },
    };
    let foreignitem = syn::ForeignItem::Fn(foreignitemfn);
    let itemforeignmod = syn::ItemForeignMod {
        attrs: vec![],
        unsafety: None,
        abi: syn::Abi {
            extern_token: syn::token::Extern {
                span: proc_macro2::Span::call_site(),
            },
            name: None,
        },
        brace_token: syn::token::Brace { span: delimspan },
        items: vec![foreignitem],
    };
    let item = syn::Item::ForeignMod(itemforeignmod);
    let file = syn::File {
        shebang: None,
        attrs: vec![],
        items: vec![item],
    };
    let pp = prettyplease::unparse(&file).trim().to_owned();
    let pp = pp.strip_prefix("extern {").unwrap_or(&pp);
    let pp = pp.strip_suffix('}').unwrap_or(pp);
    pp.trim().to_owned()
}
