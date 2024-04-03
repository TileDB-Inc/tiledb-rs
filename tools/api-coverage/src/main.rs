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
    /// The name of the API crate
    #[arg(short, long, default_value_t = String::from("tiledb"))]
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

    fn table_row<L, C, T, P>(&self, label: L, count: C, total: T, perc: P)
    where
        L: std::fmt::Display,
        C: std::fmt::Display,
        T: std::fmt::Display,
        P: std::fmt::Display,
    {
        println!(
            "<tr>\
            <th align=\"left\">{}</th>\
            <td align=\"right\">{}</td>\
            <td align=\"right\">{}</td>\
            <td align=\"right\">{}</td>\
            </tr>",
            label, count, total, perc
        )
    }

    fn report(&self) -> bool {
        let (
            mismatch_constants,
            declared_constants,
            generated_constants,
            unwrapped_constants,
        ) = self.report_constants();
        let (
            mismatch_apis,
            called_apis,
            declared_apis,
            generated_apis,
            unwrapped_apis,
            uncalled_apis,
        ) = self.report_calls();

        println!("<table>");
        println!("  <tr>");
        println!("    <th align=\"left\">Constants</th>");
        println!("    <th align=\"right\">Count</th>");
        println!("    <th align=\"right\">Total</th>");
        println!("    <th align=\"right\">Percent</th>");
        println!("  </tr>");

        self.table_row(
            "Generated",
            "",
            self.generated_defs.constants.len()
                + self.ignored_defs.constants.len(),
            "",
        );

        self.table_row("Ignored", "", self.ignored_defs.constants.len(), "");

        self.table_row("Remapped", "", self.remapped_defs.constants.len(), "");

        self.table_row(
            "Wrapped",
            declared_constants,
            generated_constants,
            format!(
                "{:.2}%",
                declared_constants as f64 / generated_constants as f64
                    * 100.0f64
            ),
        );

        println!("</table>");
        println!();

        println!("<table>");
        println!("  <tr>");
        println!("    <th align=\"left\">APIs</th>");
        println!("    <th align=\"right\">Count</th>");
        println!("    <th align=\"right\">Total</th>");
        println!("    <th align=\"right\">Percent</th>");
        println!("  </tr>");

        self.table_row(
            "Generated",
            "",
            self.generated_defs.signatures.len()
                + self.ignored_defs.signatures.len(),
            "",
        );

        self.table_row("Ignored", "", self.ignored_defs.signatures.len(), "");

        self.table_row("Remapped", "", self.remapped_defs.signatures.len(), "");

        self.table_row(
            "Declared",
            declared_apis,
            generated_apis,
            format!(
                "{:.2}%",
                declared_apis as f64 / generated_apis as f64 * 100.0f64
            ),
        );

        self.table_row(
            "Called",
            called_apis,
            generated_apis,
            format!(
                "{:.2}%",
                called_apis as f64 / generated_apis as f64 * 100.0f64
            ),
        );

        println!("</table>");
        println!();

        if !unwrapped_constants.is_empty() {
            println!("## Unwrapped Constants");
            println!();
            for name in &unwrapped_constants[..] {
                println!(" * `{}`", name);
            }
            println!();
        }

        if !unwrapped_apis.is_empty() {
            println!("## Unwrapped APIs:");
            for name in &unwrapped_apis[..] {
                println!("  * `{}`", name);
            }
            println!();
        }

        if !uncalled_apis.is_empty() {
            println!("## Uncalled APIs:");
            for name in &uncalled_apis[..] {
                println!("  * `{}`", name);
            }
            println!();
        }

        mismatch_constants || mismatch_apis
    }

    fn report_constants(&self) -> (bool, u64, u64, Vec<String>) {
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
                    println!("## Mismatched Constants");
                    println!();
                }
                mismatch = true;
                println!("<table>");
                println!(
                    "<tr><th>Generated</th><td><pre>{}</pre></td></tr>",
                    util::unparse_constant(&constant)
                );
                println!(
                    "<tr><th>Declared</th><td><pre>{}</pre></td></tr>",
                    util::unparse_constant(sys_def.unwrap())
                );
                println!("</table>");
                println!()
            }
        }

        if mismatch {
            println!();
        }

        (mismatch, declared, generated, unwrapped)
    }

    fn report_calls(&self) -> (bool, u64, u64, u64, Vec<String>, Vec<String>) {
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
                    println!("## Mismatched Function Signatures");
                    println!();
                }
                mismatch = true;
                println!("### {}", name);
                println!();
                println!("<table>");
                println!("<tr><th>Generated</th><th>Declared</th></tr>");
                println!(
                    "<tr><td><pre>{}</pre></td><td><pre>{}</pre></td></tr>",
                    util::unparse_signature(&sig),
                    util::unparse_signature(sys_def.unwrap())
                );
                println!("</table>");
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

        (mismatch, called, declared, generated, unwrapped, uncalled)
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    if Processor::new(args)?.process().report() {
        std::process::exit(1);
    }

    Ok(())
}
