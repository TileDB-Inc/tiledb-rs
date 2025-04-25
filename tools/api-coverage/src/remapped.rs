use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use syn::visit::{self, Visit};

use crate::util;

#[derive(Default)]
pub struct MappedConst {
    pub(crate) old: Option<syn::ItemConst>,
    pub(crate) new: Option<syn::ItemConst>,
}

#[derive(Default)]
pub struct MappedSignature {
    pub(crate) old: Option<syn::Signature>,
    pub(crate) new: Option<syn::Signature>,
}

#[derive(Default)]
pub struct RemappedDefs {
    pub(crate) constants: HashMap<String, MappedConst>,
    pub(crate) signatures: HashMap<String, MappedSignature>,
}

impl<'ast> Visit<'ast> for RemappedDefs {
    fn visit_item_const(&mut self, node: &'ast syn::ItemConst) {
        let ident = format!("{}", node.ident);
        // The UNINIT is used by the bindgen generated test code so we can
        // ignore it's duplicated values here.
        if ident == "UNINIT" {
            return;
        }

        let mut is_old = false;
        let ident = if ident.starts_with("old_") {
            is_old = true;
            syn::Ident::new(
                ident.strip_prefix("old_").unwrap(),
                node.ident.span(),
            )
        } else if ident.starts_with("new_") {
            assert!(!is_old);
            syn::Ident::new(
                ident.strip_prefix("new_").unwrap(),
                node.ident.span(),
            )
        } else {
            panic!(
                "Mapping names must be prefixed with \
                    `old_` or `new_`. '{ident}' is not valid."
            );
        };

        let node = syn::ItemConst {
            ident,
            ..node.clone()
        };
        let ident = format!("{}", node.ident);

        let entry = self.constants.entry(ident.clone()).or_default();

        if is_old {
            if entry.old.is_some() {
                panic!("Multiple definitions for old_{ident}");
            }
            entry.old = Some(node.clone());
        } else {
            if entry.new.is_some() {
                panic!("Multiple definitions for: new_{ident}")
            }
            entry.new = Some(node.clone());
        }

        visit::visit_item_const(self, &node);
    }

    fn visit_signature(&mut self, node: &'ast syn::Signature) {
        let ident = format!("{}", node.ident);
        if ident.starts_with("bindgen_") {
            visit::visit_signature(self, node);
            return;
        }

        let mut is_old = false;
        let ident = if ident.starts_with("old_") {
            is_old = true;
            syn::Ident::new(
                ident.strip_prefix("old_").unwrap(),
                node.ident.span(),
            )
        } else if ident.starts_with("new_") {
            assert!(!is_old);
            syn::Ident::new(
                ident.strip_prefix("new_").unwrap(),
                node.ident.span(),
            )
        } else {
            panic!(
                "Mapping names must be prefixed with \
                    `old_` or `new_`. '{ident}' is not valid.",
            );
        };

        let node = syn::Signature {
            ident,
            ..node.clone()
        };
        let ident = format!("{}", node.ident);

        let entry = self.signatures.entry(ident.clone()).or_default();

        if is_old {
            if entry.old.is_some() {
                panic!("Multiple definitions for old_{ident}");
            }
            entry.old = Some(node.clone());
        } else {
            if entry.new.is_some() {
                panic!("Multiple definitions for: new_{ident}")
            }
            entry.new = Some(node.clone());
        }

        visit::visit_signature(self, &node);
    }
}

pub fn process(remapped: &String) -> Result<RemappedDefs> {
    let path = Path::new(remapped);
    if !path.is_file() {
        return Ok(RemappedDefs::default());
    }

    let mut defs = RemappedDefs::default();
    let ast = util::parse_file(remapped).unwrap_or_else(|e| {
        panic!("Error parsing {remapped} - {e:?}");
    });
    defs.visit_file(&ast);

    Ok(defs)
}
