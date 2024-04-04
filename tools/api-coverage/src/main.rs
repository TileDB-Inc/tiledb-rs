use std::collections::{BinaryHeap, HashSet};

use anyhow::Result;
use clap::Parser;

mod api;
mod generated;
mod remapped;
mod sys;
mod util;

use generated::BindgenDefs;
use remapped::RemappedDefs;
use sys::SysDefs;

/// Generate API coverage statistics
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the API crate
    #[arg(short, long, default_value_t = String::from("tiledb/api/src"))]
    api: String,

    /// Path to the bindgen generated APIs
    #[arg(short, long, default_value_t = String::from("tiledb/sys/generated.rs"))]
    generated: String,

    /// Path to the set of ignored APIs
    #[arg(short, long, default_value_t = String::from("tiledb/sys/ignored.rs"))]
    ignored: String,

    /// Path to the remapped APIs
    #[arg(short, long, default_value_t = String::from("tiledb/sys/remapped.rs"))]
    remapped: String,

    /// Path to the sys crate
    #[arg(short, long, default_value_t = String::from("tiledb/sys/src"))]
    sys: String,

    /// Path to the wrapper.h
    #[arg(short, long, default_value_t = String::from("tiledb/sys/wrapper.h"))]
    wrapper: String,
}

#[derive(PartialEq, Eq, Clone)]
struct NamedExpr<T>
where
    T: std::cmp::PartialEq + std::cmp::Eq,
{
    name: String,
    expr: T,
}

impl<T> NamedExpr<T>
where
    T: std::cmp::PartialEq + std::cmp::Eq,
{
    fn new(name: String, expr: T) -> Self {
        Self { name, expr }
    }
}

impl<T> PartialOrd for NamedExpr<T>
where
    T: std::cmp::PartialEq + std::cmp::Eq,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for NamedExpr<T>
where
    T: std::cmp::PartialEq + std::cmp::Eq,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

struct Processor {
    api_calls: HashSet<String>,
    sys_defs: SysDefs,
    generated_defs: BindgenDefs,
    ignored_defs: BindgenDefs,
    remapped_defs: RemappedDefs,
}

impl Processor {
    fn new(args: Args) -> Result<Self> {
        Ok(Processor {
            api_calls: api::process(&args.api)?,
            sys_defs: sys::process(&args.sys)?,
            generated_defs: generated::generate(
                &args.generated,
                &args.wrapper,
            )?,
            ignored_defs: generated::process(&args.ignored)?,
            remapped_defs: remapped::process(&args.remapped)?,
        })
    }

    fn process(&mut self) -> &mut Self {
        self.ignore();
        self.remap();
        self
    }

    fn ignore(&mut self) {
        for (key, val) in self.ignored_defs.constants.iter() {
            let generated = self.generated_defs.constants.get(key);
            if generated.is_none() {
                panic!("Ignored constant was not generated: {}", key);
            }
            let generated = generated.unwrap();
            if generated != val {
                println!("Generated: {}", util::unparse_constant(generated));
                println!("Ignored:   {}", util::unparse_constant(val));
                panic!("Invalid ignore for constant: {}", key);
            }
            self.generated_defs
                .constants
                .remove(key)
                .expect("Missing constant.");
        }

        for (key, val) in self.ignored_defs.signatures.iter() {
            let generated = self.generated_defs.signatures.get(key);
            if generated.is_none() {
                panic!("Ignored signature not generated: {}", key);
            }
            let generated = generated.unwrap();
            if generated != val {
                println!("Generated: {}", util::unparse_signature(generated));
                println!("Ignored:   {}", util::unparse_signature(val));
                panic!("Invalid ignore for constant: {}", key);
            }
            self.generated_defs
                .signatures
                .remove(key)
                .expect("Missing signature.");
        }
    }

    fn remap(&mut self) {
        for (key, val) in self.remapped_defs.constants.iter() {
            if val.old.as_ref().is_none() {
                panic!("Missing old half of remapped definition for: {}", key);
            }

            if val.new.as_ref().is_none() {
                panic!("Misisng new half of remapped definition for: {}", key);
            }

            let generated = self.generated_defs.constants.get(key);
            if generated.is_none() {
                panic!("Remapped constant was not generated: {}", key);
            }
            let generated = generated.unwrap();
            if generated != val.old.as_ref().unwrap() {
                println!("Generated: {}", util::unparse_constant(generated));
                println!(
                    "Remapped:  {}",
                    util::unparse_constant(val.old.as_ref().unwrap())
                );
                panic!("Invalid remap for constant: {}", key);
            }
            self.generated_defs
                .constants
                .insert(key.clone(), val.new.as_ref().unwrap().clone());
        }

        for (key, val) in self.remapped_defs.signatures.iter() {
            let generated = self.generated_defs.signatures.get(key);
            if generated.is_none() {
                panic!("Remapped signature not generated: {}", key);
            }
            let generated = generated.unwrap();
            if generated != val.old.as_ref().unwrap() {
                println!("Generated: {}", util::unparse_signature(generated));
                println!(
                    "Remapped:  {}",
                    util::unparse_signature(val.old.as_ref().unwrap())
                );
                panic!("Invalid remap for signature: {}", key);
            }
            self.generated_defs
                .signatures
                .insert(key.clone(), val.new.as_ref().unwrap().clone());
        }
    }

    fn report(&self) -> bool {
        let (mismatch_constants, declared_constants, generated_constants) =
            self.report_constants();
        let (mismatch_apis, called_apis, declared_apis, generated_apis) =
            self.report_calls();

        println!(
            "Constants:     {} of {} ({:.2}%)",
            declared_constants,
            generated_constants,
            declared_constants as f64 / generated_constants as f64 * 100.0f64
        );

        println!(
            "Declared APIs: {} of {} ({:.2}%)",
            declared_apis,
            generated_apis,
            declared_apis as f64 / generated_apis as f64 * 100.0f64
        );

        println!(
            "Called APIs:   {} of {} ({:.2}%)",
            called_apis,
            generated_apis,
            called_apis as f64 / generated_apis as f64 * 100.0f64
        );

        mismatch_constants || mismatch_apis
    }

    fn report_constants(&self) -> (bool, u64, u64) {
        let mut unwrapped: Vec<String> = Vec::new();
        let mut declared = 0;
        let mut generated = 0;
        let mut mismatch = false;

        for node in self
            .generated_defs
            .constants
            .iter()
            .map(|(k, v)| NamedExpr::new(k.clone(), v.clone()))
            .collect::<BinaryHeap<NamedExpr<syn::ItemConst>>>()
            .into_sorted_vec()
        {
            let (name, constant) = (node.name, node.expr);
            generated += 1;

            let sys_def = self.sys_defs.constants.get(&name);
            if sys_def.is_none() {
                unwrapped.push(name);
            } else if sys_def.unwrap() == &constant {
                declared += 1;
            } else {
                if !mismatch {
                    println!("Mismatched Constants:");
                }
                mismatch = true;
                println!("  Generated: {}", util::unparse_constant(&constant));
                println!(
                    "  sys crate: {}",
                    util::unparse_constant(sys_def.unwrap())
                );
            }
        }

        if mismatch {
            println!();
        }

        if !unwrapped.is_empty() {
            println!("Unwrapped Constants:");
            for name in &unwrapped[..] {
                println!("  {}", name);
            }
            println!();
        }

        (mismatch, declared, generated)
    }

    fn report_calls(&self) -> (bool, u64, u64, u64) {
        let mut uncalled: Vec<String> = Vec::new();
        let mut unwrapped: Vec<String> = Vec::new();
        let mut called = 0;
        let mut declared = 0;
        let mut generated = 0;
        let mut mismatch = false;

        for node in self
            .generated_defs
            .signatures
            .iter()
            .map(|(k, v)| NamedExpr::new(k.clone(), v.clone()))
            .collect::<BinaryHeap<NamedExpr<syn::Signature>>>()
            .into_sorted_vec()
        {
            let (name, sig) = (node.name, node.expr);
            generated += 1;

            let mut wrapped = false;
            let sys_def = self.sys_defs.signatures.get(&name);
            if sys_def.is_none() {
                unwrapped.push(name.clone());
            } else if sys_def.unwrap() == &sig {
                wrapped = true;
                declared += 1;
            } else {
                if !mismatch {
                    println!("Mismatched Function Signatures:")
                } else {
                    println!();
                }
                mismatch = true;
                println!("{}:", name);
                println!();
                println!("  Generated:");
                println!("    {}", util::unparse_signature(&sig));
                println!();
                println!("  sys crate:");
                println!("    {}", util::unparse_signature(sys_def.unwrap()));
            }

            if wrapped {
                let ffi_call = String::from("ffi::") + &name;
                if self.api_calls.contains(&ffi_call) {
                    called += 1;
                } else {
                    uncalled.push(name);
                }
            }
        }

        if mismatch {
            println!();
        }

        if !unwrapped.is_empty() {
            println!("Unwrapped APIs:");
            for name in &unwrapped[..] {
                println!("  {}", name);
            }
            println!();
        }

        if !uncalled.is_empty() {
            println!("Uncalled APIs:");
            for name in &uncalled[..] {
                println!("  {}", name);
            }
            println!();
        }

        (mismatch, called, declared, generated)
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    if Processor::new(args)?.process().report() {
        std::process::exit(1);
    }

    Ok(())
}
