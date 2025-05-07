#[cxx::bridge(namespace = "tiledb::rs")]
mod ffi {
    pub enum FilterType {
        None,
        Gzip,
        Zstd,
        Lz4,
        Rle,
        Bzip2,
        DoubleDelta,
        BitWidthReduction,
        BitShuffle,
        ByteShuffle,
        PositiveDelta,
        ChecksumMd5,
        ChecksumSha256,
        Dictionary,
        ScaleFloat,
        Xor,
        Deprecated,
        WebP,
        Delta,
    }
}

pub use ffi::*;
