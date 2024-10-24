TileDB - Rust Bindings
===

Rust bindings for TileDB. (currently covering ~45% of the TileDB C API).

Getting Started
---

This repository is responsible for building Rust bindings to the libtiledb
embedded library. This means that we have to ensure that libtiledb is able
to be linked into the resulting Rust binaries. This repository supports both
dynamic and static linking against libtiledb.

Controlling Linkage
---

By default, `tiledb-sys` will attempt to link `libtiledb` dynamically. To enable
static linking, `tiledb-sys` requires that an environment variable is set:

```sh
$ export TILEDB_SYS_STATIC=true
```

Dynamic Linking
---

When linking dynamically, its up to the user of this repository to have
prepared the system state such that `libtiledb.{so,dylib,dll}` are compiled and
discoverable via `pkg-config`.

You can use `pkg-config` to check if your configuration is usable:

```sh
$ pkg-config tiledb --libs
-L/opt/tiledb/lib -ltiledb
```

When building `libtiledb` with dynamic linkage, it is recommended to enable
S3 and Serialization support like such:

```sh
$ cd ~/tiledb/
$ mkdir build
$ cd build
$ ../bootstrap --enable=s3,serialization,debug --prefix=/some/path
$ make -j$(nproc)
$ make -j$(nproc) install
```

Once build, make sure that your `PKG_CONFIG_PATH` to include the path where
libtiledb was installed:

```sh
if [[ ":$PKG_CONFIG_PATH:" != *":/some/path/lib/pkgconfig:"* ]]; then
  export "PKG_CONFIG_PATH=${PKG_CONFIG_PATH:+${PKG_CONFIG_PATH}:}/some/path/lib/pkgconfig"
fi
```

> [!NOTE]
> The snippet above likely looks overly complicated for setting an environment
> variable. The reason for this is that `cargo` will invalidate cached builds
> if any of the inputs change, including the value of `PKG_CONFIG_PATH`. The
> shell weirdness above just ensures that we don't add duplicates to the path
> which can cause unnecessary rebuilds of anything that transitively depends
> on `tiledb-sys`.

Static Linking
---

First, we need to opt into static linkage:

```sh
$ export TILEDB_SYS_STATIC=true
```

At this point, building a statically linked Rust binary should Just Work®.

> [!WARNING]
> If you attempt to develop anything using `tiledb-sys` outside of CI
> pipelines, you'll probably have a Not Very Good Time between `rust-analyzer`
> and compiler loop times. Also, anything like `cargo clean` or slight
> dependency changes will cause a complete rebuild of libtiledb.

Static Linking for Developers
---

For those of us developing this repository or the downstream users of the
`tiledb-sys` crate, there's an environment variable to specify a path for
building `libtiledb`. This allows for developers to both avoid invalidating
their `libtiledb` builds as well as provide an easy way to use variant
`libtiledb` builds when debugging.

Configuring this setup is as easy as:

```sh
$ export TILEDB_SYS_OUT_DIR=/some/path/to/scratch/directory
```

> [!IMPORTANT]
> Using this configuration means that you are responsible for the contents of
> `TILEDB_SYS_OUT_DIR` and ensuring they are compatible with the version of
> `tiledb-sys` that your are attempting to build.

The value of `TILEDB_SYS_OUT_DIR` must be a directory that exists, it will not
be automatically created to avoid any pathological rebuild issues.

The contents of `TILEDB_SYS_OUT_DIR` that `tiledb-sys` cares about are two
subdirectories: `git` and `build`. By defualt, `tiledb-sys` will auto-populate
these and will then re-use the contents trusting that they remain valid between
builds.

If you so choose, you are free to create these directories by hand to setup
any special environments or testing setups for debugging. There are two main
requirements:

1. `${TILEDB_SYS_OUT_DIR}/git` must be a clone of `TileDB-Inc/TileDB` or a fork.
2. `${TILEDB_SYS_OUT_DIR}/build` must be a CMake build directory with static linkage.

An example shell session of creating these directories as follows. This is
more or less how `tiledb-sys` will attempt to build `libtiledb` if left to
fend for itself. However, `libtildb` will never update the `git` subdirectory
and will avoid invoking `cmake` if it detects its own build output.

> [!NOTE]
> If `tiledb-sys` starts failing to compile correctly, the state of this
> directory can be wiped to double check if its just a new incompatibility with
> your cached build.

Creating a `TILEDB_SYS_OUT_DIR` by hand:

```sh
$ export TILEDB_SYS_OUT_DIR=/some/path/to/code
$ mkdir $TILEDB_SYS_OUT_DIR
$ cd $TILEDB_SYS_OUT_DIR
$ git clone https://github.com/TileDB-Inc/TileDB git
$ mkdir build
$ cd build
$ ../git/bootstrap --enable=ccache,s3,serialization,debug --linkage=static
$ make -j$(nproc)
```

`TILEDB_SYS_JOBS`
---

If you let `tiledb-sys` build `libtiledb`, you can export `TILEDB_SYS_JOBS=$N`
to control the CMake parallelism used.
