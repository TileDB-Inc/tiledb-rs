use std::collections::HashMap;
use std::path::Path;

use anyhow::{Result, anyhow};
use syn::visit::{self, Visit};

use crate::util;

#[derive(Default)]
pub struct BindgenDefs {
    pub(crate) constants: HashMap<String, syn::ItemConst>,
    pub(crate) signatures: HashMap<String, syn::Signature>,
}

impl<'ast> Visit<'ast> for BindgenDefs {
    fn visit_item_const(&mut self, node: &'ast syn::ItemConst) {
        let ident = format!("{}", node.ident);
        // The UNINIT is used by the bindgen generated test code so we can
        // ignore it's duplicated values here.
        if ident == "UNINIT" {
            return;
        }
        if self.constants.contains_key(&ident) {
            panic!("Duplicate constant defintion: {ident}");
        }

        // Drop all attributes on const definitions to avoid spurious
        // mismatches when the C API generates documentation attributes.
        let node = syn::ItemConst {
            attrs: vec![],
            ..node.clone()
        };

        self.constants.insert(ident, node.clone());
        visit::visit_item_const(self, &node);
    }

    fn visit_signature(&mut self, node: &'ast syn::Signature) {
        let ident = format!("{}", node.ident);
        if ident.starts_with("bindgen_") {
            visit::visit_signature(self, node);
            return;
        }
        if self.signatures.contains_key(&ident) {
            panic!("Duplicate signature definition: {ident}");
        }
        self.signatures.insert(ident, node.clone());
        visit::visit_signature(self, node);
    }
}

fn generate_bindings(generated: &String, wrapper: &String) -> Result<()> {
    // Only generate bindings if they don't exist.
    let genpath = Path::new(&generated);
    if genpath.is_file() {
        return Ok(());
    }

    let wrappath = Path::new(&wrapper);
    if !wrappath.is_file() {
        return Err(anyhow!("Missing wrapper file: {wrapper}"));
    }

    let tiledb_lib = pkg_config::Config::new()
        .cargo_metadata(false)
        .env_metadata(false)
        .probe("tiledb")?;

    let mut bindings = bindgen::Builder::default()
        .header(wrapper)
        .allowlist_function("^tiledb_.*")
        .allowlist_type("^tiledb_.*")
        .allowlist_var("^TILEDB_.*");

    for path in tiledb_lib.include_paths.iter() {
        bindings = bindings.clang_arg(format!("-I{}", path.to_string_lossy()));
    }

    let bindings = bindings.generate().expect("Error generating bindings!");

    bindings
        .write_to_file(genpath)
        .expect("Error writing bindings to disk");

    Ok(())
}

pub fn generate(generated: &String, wrapper: &String) -> Result<BindgenDefs> {
    generate_bindings(generated, wrapper)?;

    let mut bindgen = BindgenDefs::default();
    let ast = util::parse_file(generated).unwrap_or_else(|e| {
        panic!("Error parsing {generated} - {e:?}");
    });
    bindgen.visit_file(&ast);

    Ok(bindgen)
}

pub fn process(ignored: &String) -> Result<BindgenDefs> {
    let path = Path::new(ignored);
    if !path.is_file() {
        return Ok(BindgenDefs::default());
    }

    let mut bindgen = BindgenDefs::default();
    let ast = util::parse_file(ignored).unwrap_or_else(|e| {
        panic!("Error parsing {ignored} - {e:?}");
    });
    bindgen.visit_file(&ast);

    Ok(bindgen)
}
