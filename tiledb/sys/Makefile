
all: todo

generated.rs:
	bindgen \
    -o generated.rs \
    --allowlist-function '^tiledb_.*' \
    --allowlist-var '^TILEDB_.*' \
    wrapper.h \
    -- -I/opt/tiledb/include

generated-functions.txt: generated.rs
	rg -o --no-line-number --no-filename '\bfn\s+tiledb_[^(]+' generated.rs | sort > generated-functions.txt

covered-functions.txt:
	rg -o --no-line-number --no-filename '\bfn\s+tiledb_[^(]+' src/*.rs | sort > covered-functions.txt

todo: generated-functions.txt covered-functions.txt
	comm -23 generated-functions.txt covered-functions.txt > todo.txt
	cat todo.txt
