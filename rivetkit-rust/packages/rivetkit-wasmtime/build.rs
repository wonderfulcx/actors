use std::fs;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
	let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
	let action_schema_dir = manifest_dir.join("action-schemas");
	vbare_compiler::process_schemas_with_config(
		&action_schema_dir,
		&vbare_compiler::Config::default(),
	)?;
	// The compiler shares OUT_DIR across schema sets. Namespace action types before
	// generating effects, whose existing combined_imports.rs must remain unchanged.
	for version in [1, 2] {
		fs::rename(
			out_dir.join(format!("v{version}_generated.rs")),
			out_dir.join(format!("action_v{version}_generated.rs")),
		)?;
	}

	let schema_dir = manifest_dir.join("schemas");
	println!("cargo:rerun-if-changed={}", schema_dir.display());
	vbare_compiler::process_schemas_with_config(&schema_dir, &vbare_compiler::Config::default())?;
	Ok(())
}
