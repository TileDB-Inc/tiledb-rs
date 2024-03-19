
# You might have to `cargo install bindgen-cli` for this to work
bindings:
	bindgen \
    -o generated.rs \
    --allowlist-function '^tiledb_.*' \
    --allowlist-var '^TILEDB_.*' \
    wrapper.h \
    -- -I/opt/tiledb/include
