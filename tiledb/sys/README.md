tiledb-sys
===

This crate contains the raw wrapped function definitions that are then used
by the `tiledb/api` crate to provide a usable Rust API to TileDB. Nothing in
this crate is intended for use by anything other than `tiledb/api`.

Listing Unwrapped APIs
===

If you're looking to contribute to this repository, the easiest approach to
looking for things to wrap is to change directories into the directory
containing this file and run `make`. This will dump a list of unwrapped
functions.

Requirements
---

To generate the todo list of functions to wrap, you need to have installed
both bindgen and ripgrep. On macOS, the easiest way to acquire these
is to run the following:

```bash
$ cargo install bindgen-cli
$ brew install ripgrep
```
