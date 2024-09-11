pub mod config {
    use std::ops::Deref;
    use std::sync::LazyLock;

    /// The value of a strategy configuration parameter and its provenance.
    pub enum Configuration<T> {
        Default(T),
        Environmental(T),
    }

    impl<T> Configuration<T> {
        /// Converts to [Option<T>], returning the wrapped value
        /// if this is [Environmental] and [None] otherwise.
        pub fn environmental(&self) -> Option<T>
        where
            T: Copy,
        {
            match self {
                Self::Default(_) => None,
                Self::Environmental(value) => Some(*value),
            }
        }
    }

    impl<T> Deref for Configuration<T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            match self {
                Self::Default(ref value) => value,
                Self::Environmental(ref value) => value,
            }
        }
    }

    macro_rules! config_param {
        ($name:ident, $type:ty, $default:expr) => {
            pub static $name: LazyLock<Configuration<$type>> =
                LazyLock::new(|| {
                    if let Some(value) =
                        crate::env::parse::<$type>(stringify!($name))
                    {
                        Configuration::Environmental(value)
                    } else {
                        Configuration::Default($default)
                    }
                });
        };
    }

    config_param!(TILEDB_STRATEGY_DOMAIN_PARAMETERS_DIMENSIONS_MIN, usize, 1);
    config_param!(TILEDB_STRATEGY_DOMAIN_PARAMETERS_DIMENSIONS_MAX, usize, 8);
    config_param!(
        TILEDB_STRATEGY_DOMAIN_PARAMETERS_CELLS_PER_TILE_LIMIT,
        usize,
        1024 * 32
    );
}
