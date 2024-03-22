TileDB - Rust Bindings
===

Rust bindings for TileDB. Currently covering 0.5% of the API.

Getting Started
---

For the time being, these bindings require that libtiledb be installed into
`/opt/tiledb`. Eventually we'll fix the linking issues to not require this
but for now it was the easiest to get working with Cargo.

On macOS and Linux, these quick instructions should be enough to get
`cargo test` running:

```sh
$ cd ~/wherever/you/keep/code
$ git clone https://github.com/TileDB-Inc/TileDB
$ cd TileDB
$ mkdir build
$ cd build
$ ../bootstrap --enable=ccache,serialization,debug --prefix=/opt/tiledb
$ make -j$(nproc) && make -C tiledb -j$(nproc) install
$ cd ~/wherever/you/keep/code
$ git clone https://github.com/TileDB-Inc/tiledb-rs
$ cd tiled-rs
$ cargo test
```
