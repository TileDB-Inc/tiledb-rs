use std::collections::HashMap;

use anyhow::Result;
use syn::visit::{self, Visit};

use crate::util;

/// Used by SysVisitor to get the declared API wrappers.
#[derive(Default)]
pub struct SysDefs {
    pub(crate) constants: HashMap<String, syn::ItemConst>,
    pub(crate) signatures: HashMap<String, syn::Signature>,
}

impl<'ast> syn::visit::Visit<'ast> for SysDefs {
    fn visit_item_const(&mut self, node: &'ast syn::ItemConst) {
        let ident = format!("{}", node.ident);
        if self.constants.get(&ident).is_some() {
            panic!("Error: Duplicate constant definition: {}", ident);
        }
        self.constants.insert(ident, node.clone());
        visit::visit_item_const(self, node);
    }

    fn visit_signature(&mut self, node: &'ast syn::Signature) {
        let ident = format!("{}", node.ident);
        if self.signatures.get(&ident).is_some() {
            panic!("Error: Duplicate function signature: {}", ident);
        }
        self.signatures.insert(ident, node.clone());
        visit::visit_signature(self, node);
    }
}

pub fn process(path: &String) -> Result<SysDefs> {
    let mut sys = SysDefs::default();

    util::walk_rust_sources(path, |src| {
        let ast = util::parse_file(&src).unwrap_or_else(|e| {
            panic!("Error parsing {} - {:?}", src, e);
        });
        sys.visit_file(&ast);
    });

    Ok(sys)
}
