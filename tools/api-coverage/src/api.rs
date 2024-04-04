use std::collections::HashSet;

use anyhow::Result;
use syn::visit::{self, Visit};

use crate::util;

/// Used by APIVisitor to get the function name being called.
#[derive(Default)]
struct FnNameVisitor {
    segments: Vec<String>,
}

impl<'ast> Visit<'ast> for FnNameVisitor {
    fn visit_path_segment(&mut self, node: &'ast syn::PathSegment) {
        let ident = format!("{}", node.ident);
        self.segments.push(ident);
    }
}

/// A visitor to collect all function calls in the API crate.
#[derive(Default)]
struct APIVisitor {
    calls: HashSet<String>,
}

impl<'ast> Visit<'ast> for APIVisitor {
    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let syn::Expr::Path(path) = node.func.as_ref() {
            let mut fnname = FnNameVisitor::default();
            visit::visit_expr_path(&mut fnname, path);
            let fnname = fnname.segments.join("::");
            self.calls.insert(fnname);
        } else {
            panic!("Unexpected function invocation syntax: {:?}", node.func);
        }
    }
}

pub fn process(path: &String) -> Result<HashSet<String>> {
    let mut api = APIVisitor::default();

    util::walk_rust_sources(path, |src| {
        let ast = util::parse_file(&src).unwrap_or_else(|e| {
            panic!("Error parsing {} - {:?}", src, e);
        });
        api.visit_file(&ast);
    });

    Ok(api.calls)
}
