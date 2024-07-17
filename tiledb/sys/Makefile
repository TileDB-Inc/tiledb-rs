
all: todo

generated.rs:
	bindgen \
    -o generated.rs \
    --allowlist-function '^tiledb_.*' \
    --allowlist-var '^TILEDB_.*' \
    wrapper.h \
    -- -I/opt/tiledb/include

generated-functions.txt: generated.rs
    # Which functions are exponsed by the C++ API.
	rg -o --no-line-number --no-filename '\bfn\s+tiledb_[^(]+' generated.rs | sort > generated-functions.txt

covered-functions.txt:
    # Which functions are already implemented.
	rg -o --no-line-number --no-filename '\bfn\s+tiledb_[^(]+' src/*.rs | sort > covered-functions.txt

ignored-functions.txt:
    # Which functions we do not wish to implement.
	rg -o --no-line-number --no-filename '\bfn\s+tiledb_[^(]+' ignored.rs | sort > ignored-functions.txt

todo: generated-functions.txt covered-functions.txt ignored-functions.txt
	comm -23 generated-functions.txt covered-functions.txt > not-covered.txt
	comm -23 not-covered.txt ignored-functions.txt > todo.txt
	comm -12 covered-functions.txt ignored-functions.txt > ignored-but-covered.txt
	cat todo.txt

