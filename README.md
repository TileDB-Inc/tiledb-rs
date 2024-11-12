TileDB - Rust Bindings
===

Rust bindings for TileDB. (currently covering ~56% of the TileDB C API).

Getting Started
---

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

Creating Static Binaries
---

> [!WARNING]
> It is highly recommended to use the dynamic linking as described above unless
> you are specifically working on creating statically linked release builds
> for distribution. Building statically can take on the order of thirty minutes
> and these builds are easily invalidated requiring complete rebuilds. This ends
> up leading to an extremely poor developer experience.

To build libtiledb statically, simply set the `TILEDB_SYS_STATIC` environment
variable to anything.

```sh
$ export TILEDB_SYS_STATIC=true
$ cargo build
```

> [!NOTE]
> If you are encountering "weird" failures in CI where the libtiledb build
> appears to error out for no reason, it is likely that `cmake` is being too
> aggressive in parallelizing compilation jobs. See the `TILEDB_SYS_JOBS`
> environment variable below.

Controlling Static Builds
---

There are a few environment variables you can use to attempt to speed up static
builds.

* `TILEDB_SYS_JOBS` - If this environment variable is set, it is passed as
  `-j${TILEDB_SYS_JOBS}` to `cmake`. This can also be useful to limit
  parallelization in CI where the compiler can end up starving the CI runner
  of RAM which can result in mysteriously failed builds.
* `TILEDB_SYS_CCACHE` - You can set this to anything to tell libtiledb to search
  for either `sccache` or `ccache` while building. Consult documentation for
  either of those tools if you wish to install them. You'll likely want to use
  `sccache`.
* `VCPKG_ROOT` - Setting up an external installation of `vcpkg` will ensure that
  libtiledb dependencies are cached which speeds up builds tremendously. Consult
  the `vcpkg` documentation for specifics. Though the gist of it is to clone the
  vcpkg repository, run the bootstrap script to download binaries, then export
  the `VCPKG_ROOT` environment variable to point at that directory.

