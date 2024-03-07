TileDB - Rust Bindings
===

Rust bindings for TileDB. Currently covering 0.5% of the API.


Note to macOS Users
---

I have `libtiledb.dylib` installed in `/opt/tiledb/lib` which is not on the
default linker search path and not covered by Cargo's definition of
`DYLD_FALLBACK_LIBRARY_PATH`. Thus, I had to create a `~/.cargo/config.toml`
with these contents:

```
[build]
rustflags = ["-C", "link-args=-Wl,-rpath,/opt/tiledb/lib"]
```

I'd really like to figure out how to avoid doing this. I'm guessing I'll
probably have to eventually merge it into a Homebrew linked cask thinger and
then rely on Homebrew search paths? And/or maybe we need to patch TileDB's
CMake scripts for macOS library location?
