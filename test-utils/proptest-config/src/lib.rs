use std::ops::Deref;
use std::str::FromStr;
use std::sync::LazyLock;

fn try_parse_env<T>(env: &str) -> Option<T>
where
    T: FromStr,
{
    match std::env::var(env) {
        Ok(value) => Some(
            T::from_str(&value)
                .unwrap_or_else(|_| panic!("Invalid value for {}", env)),
        ),
        Err(_) => None,
    }
}

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
                if let Some(value) = try_parse_env::<$type>(stringify!($name)) {
                    Configuration::Environmental(value)
                } else {
                    Configuration::Default($default)
                }
            });
    };
}

// array/attribute/strategy.rs
config_param!(
    TILEDB_STRATEGY_ATTRIBUTE_PARAMETERS_ENUMERATION_LIKELIHOOD,
    f64,
    0.2
);

// array/domain/strategy.rs
config_param!(TILEDB_STRATEGY_DOMAIN_PARAMETERS_DIMENSIONS_MIN, usize, 1);
config_param!(TILEDB_STRATEGY_DOMAIN_PARAMETERS_DIMENSIONS_MAX, usize, 8);
config_param!(
    TILEDB_STRATEGY_DOMAIN_PARAMETERS_CELLS_PER_TILE_LIMIT,
    usize,
    1024 * 32
);

// array/schema/strategy.rs
config_param!(TILEDB_STRATEGY_SCHEMA_PARAMETERS_ATTRIBUTES_MIN, usize, 1);
config_param!(TILEDB_STRATEGY_SCHEMA_PARAMETERS_ATTRIBUTES_MAX, usize, 8);
config_param!(
    TILEDB_STRATEGY_SCHEMA_PARAMETERS_SPARSE_TILE_CAPACITY_MIN,
    u64,
    1
);
config_param!(
    TILEDB_STRATEGY_SCHEMA_PARAMETERS_SPARSE_TILE_CAPACITY_MAX,
    u64,
    **TILEDB_STRATEGY_DOMAIN_PARAMETERS_CELLS_PER_TILE_LIMIT as u64
);

// array/enumeration/strategy.rs
config_param!(
    TILEDB_STRATEGY_ENUMERATION_PARAMETERS_NUM_VARIANTS_MIN,
    usize,
    1
);
config_param!(
    TILEDB_STRATEGY_ENUMERATION_PARAMETERS_NUM_VARIANTS_MAX,
    usize,
    1024
);
config_param!(
    TILEDB_STRATEGY_ENUMERATION_PARAMETERS_VAR_VARIANT_NUM_VALUES_MIN,
    usize,
    0
);
config_param!(
    TILEDB_STRATEGY_ENUMERATION_PARAMETERS_VAR_VARIANT_NUM_VALUES_MAX,
    usize,
    64
);

// query/strategy.rs
config_param!(TILEDB_STRATEGY_CELLS_PARAMETERS_NUM_RECORDS_MIN, usize, 0);
config_param!(TILEDB_STRATEGY_CELLS_PARAMETERS_NUM_RECORDS_MAX, usize, 16);
config_param!(TILEDB_STRATEGY_CELLS_PARAMETERS_CELL_VAR_SIZE_MIN, usize, 0);
config_param!(
    TILEDB_STRATEGY_CELLS_PARAMETERS_CELL_VAR_SIZE_MAX,
    usize,
    16
);

// query/write/strategy.rs
config_param!(
    TILEDB_STRATEGY_DENSE_WRITE_PARAMETERS_MEMORY_LIMIT,
    usize,
    16 * 1024 // chosen arbitrarily; seems small
);
config_param!(
    TILEDB_STRATEGY_WRITE_SEQUENCE_PARAMETERS_MIN_WRITES,
    usize,
    1
);
config_param!(
    TILEDB_STRATEGY_WRITE_SEQUENCE_PARAMETERS_MAX_WRITES,
    usize,
    8
);
