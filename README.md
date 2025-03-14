# TileDB - Rust Bindings

Rust bindings for TileDB. (currently covering ~56% of the TileDB C API).

## Getting Started

Using these bindings requires having a copy of libtiledb installed on your
system where `pkg-config` can find it. There are many different ways this
can be accomplished. Building from source is shown below for clarity. For other
installation methods, see the TileDB documentation.

In this example, I'm using `/opt/tiledb` for the installation location. You are
free to use any path you so desire.

```sh
$ sudo mkdir -p /opt/tiledb
$ sudo chown your_username /opt/tiledb
$ git clone https://github.com/TileDB-Inc/TileDB tiledb
$ cd ~/tiledb/
$ mkdir build
$ cd build
$ ../bootstrap --enable=s3,serialization,debug --prefix=/opt/tiledb
$ make -j$(nproc)
$ make -j$(nproc) install
```

Once built, make sure that your `PKG_CONFIG_PATH` includes the path where
libtiledb was installed:

```sh
if [[ ":$PKG_CONFIG_PATH:" != *":/opt/tiledb/lib/pkgconfig:"* ]]; then
  export "PKG_CONFIG_PATH=${PKG_CONFIG_PATH:+${PKG_CONFIG_PATH}:}/opt/tiledb/lib/pkgconfig"
fi
```

> [!NOTE]
> The snippet above likely looks overly complicated for setting an environment
> variable. The reason for this is that `cargo` will invalidate cached builds
> if any of the inputs change, including the value of `PKG_CONFIG_PATH`. The
> shell weirdness above just ensures that we don't add duplicates to the path
> which can cause unnecessary rebuilds of anything that transitively depends
> on `tiledb-sys`.
>
> An easy way to check if your Rust environment is causing a bunch of
> unnecessary build churn is by using sub-shells:
>
> ```sh
> $ cargo build
> $ zsh # or whatever your shell happens to be
> $ cargo build
> ```
>
> If that second `cargo build` command causes anything at all to be built, you
> likely have something in your environment that's being mutated on every
> invocation.

Finally, we can check that everything compiled and can be discovered by
`pkg-config`:

```sh
$ pkg-config tiledb --libs
-L/opt/tiledb/lib -ltiledb
```

## Creating Static Binaries

> [!WARNING]
> Generally speaking, you likely want to be using dynamically linked libtiledb
> unless you're working on distributing binaries.

To build `tiledb-rs` with static linkage to TileDB, you just need to have a
copy of `libtiledb_static.a` located in the `lib` directory where TileDB is
installed. The easiest way to accomplish this is to download a static package
from [the nightlies release][1].

If you're interested in how to generate `libtiledb_static.a`, you can find that
logic in the `Nightly TileDB Packages` workflow in
`.github/workflows/packages.yml`.

[1]: https://github.com/TileDB-Inc/tiledb-rs/releases/tag/nightlies
