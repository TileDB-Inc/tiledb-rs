use std::collections::HashSet;
use std::process::{Command, Stdio};

use anyhow::Result;
use syn::visit::{self, Visit};

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

pub fn process(name: &String) -> Result<HashSet<String>> {
    let output = Command::new("cargo")
        .arg("expand")
        .arg("-p")
        .arg(name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap_or_else(|_| panic!("Failed to run `cargo expand {}`", name));

    if !output.status.success() {
        panic!(
            "Failed to run `cargo exapnd -p {}`\n\n\
            *NOTE* You may need to run: `cargo install cargo-expand`\n",
            name
        );
    }

    let source = String::from_utf8(output.stdout).unwrap();
    let ast = syn::parse_file(&source)?;
    let mut api = APIVisitor::default();
    api.visit_file(&ast);

    Ok(api.calls)
}
