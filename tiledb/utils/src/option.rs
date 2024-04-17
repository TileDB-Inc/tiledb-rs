use std::num::*;

/// Trait for comparing types which can express optional data.
/// The `option_subset` method should return true if it finds that all required data
/// of two objects are equal, and the non-required data are either equal or not set
/// for the method receiver.  For objects which only have required data, this should be
/// the same as `eq` - you can use the `option_subset_partialeq` macro as a convenience
/// to use this implementation.
///
/// The target usage of this trait is data with optional fields
/// which could be filled in with arbitrary values across some logical boundary.
/// If we were to compare representations of the same object before and after the boundary,
/// then it would not be "equal" to itself because of values which may have been filled in.
///
/// ## Derivable
///
/// This trait can be used with `#[derive]`.
/// When `derived` on structs, one instance of the struct is an `option_subset` of another instance
/// if all fields are `option_subsets` of the corresponding fields of the other instance.
pub trait OptionSubset {
    fn option_subset(&self, other: &Self) -> bool;
}

#[macro_export]
macro_rules! option_subset_partialeq {
    ($($T:ty),+) => {
        $(
            impl $crate::option::OptionSubset for $T {
                fn option_subset(&self, other: &Self) -> bool {
                    self == other
                }
            }
        )+
    };
}

#[macro_export]
macro_rules! assert_option_subset {
    ($left:expr, $right:expr $(,)?) => {
        match (&$left, &$right) {
            (left_val, right_val) => {
                if !(left_val.option_subset(right_val)) {
                    panic!(
                        r#"assertion `(left).option_subset(right)` failed
 left: {left_val:?}
right: {right_val:?}"#
                    )
                }
            }
        }
    };
}

#[macro_export]
macro_rules! assert_not_option_subset {
    ($left:expr, $right:expr $(,)?) => {
        match (&$left, &$right) {
            (left_val, right_val) => {
                if left_val.option_subset(right_val) {
                    panic!(
                        r#"assertion `!(left).option_subset(right)` failed
 left: {left_val:?}
right: {right_val:?}"#
                    )
                }
            }
        }
    };
}

impl<T> OptionSubset for Option<T>
where
    T: OptionSubset,
{
    fn option_subset(&self, other: &Self) -> bool {
        match (self, other) {
            (None, None) => true,
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some(mine), Some(theirs)) => mine.option_subset(theirs),
        }
    }
}

impl<T> OptionSubset for [T]
where
    T: OptionSubset,
{
    fn option_subset(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .zip(other.iter())
                .all(|(mine, theirs)| mine.option_subset(theirs))
    }
}

impl<T, const K: usize> OptionSubset for [T; K]
where
    T: OptionSubset,
{
    fn option_subset(&self, other: &Self) -> bool {
        self.as_slice().option_subset(other.as_slice())
    }
}

impl<T> OptionSubset for Vec<T>
where
    T: OptionSubset,
{
    fn option_subset(&self, other: &Self) -> bool {
        self.as_slice().option_subset(other.as_slice())
    }
}

#[cfg(feature = "serde_json")]
mod serde_json {
    use super::*;
    use crate::serde_json::value::{Map, Number, Value};

    option_subset_partialeq!(Number);

    impl OptionSubset for Map<String, Value> {
        fn option_subset(&self, other: &Self) -> bool {
            for (k, self_v) in self.iter() {
                if let Some(other_v) = other.get(k) {
                    if !self_v.option_subset(other_v) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            true
        }
    }

    impl OptionSubset for Value {
        fn option_subset(&self, other: &Self) -> bool {
            match (self, other) {
                (Value::Null, _) => true, /* sorry SQL folks */
                (Value::Bool(lb), Value::Bool(rb)) => lb.option_subset(rb),
                (Value::Number(ln), Value::Number(rn)) => ln.option_subset(rn),
                (Value::String(ls), Value::String(rs)) => ls.option_subset(rs),
                (Value::Array(lv), Value::Array(rv)) => lv.option_subset(rv),
                (Value::Object(lo), Value::Object(ro)) => lo.option_subset(ro),
                _ => false,
            }
        }
    }
}

option_subset_partialeq!(u8, u16, u32, u64, u128, usize);
option_subset_partialeq!(i8, i16, i32, i64, i128, isize);
option_subset_partialeq!(bool, f32, f64, String);
option_subset_partialeq!(
    NonZeroI8,
    NonZeroI16,
    NonZeroI32,
    NonZeroI64,
    NonZeroI128,
    NonZeroIsize
);
option_subset_partialeq!(
    NonZeroU8,
    NonZeroU16,
    NonZeroU32,
    NonZeroU64,
    NonZeroU128,
    NonZeroUsize
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit() {
        #[derive(Debug, OptionSubset)]
        struct Unit;

        assert_option_subset!(Unit, Unit);
    }

    #[test]
    fn point_unnamed() {
        #[derive(Debug, OptionSubset)]
        struct PointUnnamed(i64, i64);

        assert_option_subset!(PointUnnamed(0, 0), PointUnnamed(0, 0));
        assert_not_option_subset!(PointUnnamed(0, 0), PointUnnamed(0, 1));
    }

    #[derive(Debug, OptionSubset)]
    struct PointNamed {
        x: i64,
        y: i64,
    }

    impl PointNamed {
        pub fn new(x: i64, y: i64) -> Self {
            PointNamed { x, y }
        }
    }

    #[test]
    fn point_named() {
        assert_option_subset!(PointNamed::new(0, 0), PointNamed::new(0, 0));
        assert_not_option_subset!(PointNamed::new(0, 0), PointNamed::new(0, 1));
    }

    #[test]
    fn point_kd_named() {
        #[derive(Debug, OptionSubset)]
        struct PointKDNamed {
            axes: Vec<i64>,
        }

        impl PointKDNamed {
            pub fn new(axes: Vec<i64>) -> Self {
                PointKDNamed { axes }
            }
        }

        assert_option_subset!(
            PointKDNamed::new(vec![]),
            PointKDNamed::new(vec![])
        );
        assert_option_subset!(
            PointKDNamed::new(vec![0]),
            PointKDNamed::new(vec![0])
        );
        assert_option_subset!(
            PointKDNamed::new(vec![0, 0]),
            PointKDNamed::new(vec![0, 0])
        );
        assert_not_option_subset!(
            PointKDNamed::new(vec![0, 0]),
            PointKDNamed::new(vec![0])
        );
        assert_not_option_subset!(
            PointKDNamed::new(vec![0, 0]),
            PointKDNamed::new(vec![0, 0, 0])
        );
        assert_not_option_subset!(
            PointKDNamed::new(vec![0, 0]),
            PointKDNamed::new(vec![0, 1])
        );
    }

    #[test]
    fn point_kd_unnamed() {
        #[derive(Debug, OptionSubset)]
        struct PointKDUnnamed(Vec<i64>);

        assert_option_subset!(PointKDUnnamed(vec![]), PointKDUnnamed(vec![]));
        assert_option_subset!(PointKDUnnamed(vec![0]), PointKDUnnamed(vec![0]));
        assert_option_subset!(
            PointKDUnnamed(vec![0, 0]),
            PointKDUnnamed(vec![0, 0])
        );
        assert_not_option_subset!(
            PointKDUnnamed(vec![0, 0]),
            PointKDUnnamed(vec![0])
        );
        assert_not_option_subset!(
            PointKDUnnamed(vec![0, 0]),
            PointKDUnnamed(vec![0, 0, 0])
        );
        assert_not_option_subset!(
            PointKDUnnamed(vec![0, 0]),
            PointKDUnnamed(vec![0, 1])
        );
    }

    #[test]
    fn point_or_plane_named() {
        #[derive(Debug, OptionSubset)]
        struct PointOrPlane {
            x: Option<i64>,
            y: Option<i64>,
        }

        let everything = PointOrPlane { x: None, y: None };
        let y_axis = PointOrPlane {
            x: Some(0),
            y: None,
        };
        let x_axis = PointOrPlane {
            x: None,
            y: Some(0),
        };
        let origin = PointOrPlane {
            x: Some(0),
            y: Some(0),
        };
        let x_point = PointOrPlane {
            x: Some(1),
            y: Some(0),
        };
        let y_point = PointOrPlane {
            x: Some(0),
            y: Some(1),
        };
        let point = PointOrPlane {
            x: Some(1),
            y: Some(1),
        };

        assert_option_subset!(everything, everything);
        assert_option_subset!(everything, y_axis);
        assert_option_subset!(everything, x_axis);
        assert_option_subset!(everything, origin);
        assert_option_subset!(everything, y_point);
        assert_option_subset!(everything, x_point);
        assert_option_subset!(everything, point);

        assert_option_subset!(y_axis, y_axis);
        assert_option_subset!(y_axis, origin);
        assert_option_subset!(y_axis, y_point);
        assert_not_option_subset!(y_axis, everything);
        assert_not_option_subset!(y_axis, x_axis);
        assert_not_option_subset!(y_axis, x_point);
        assert_not_option_subset!(y_axis, point);

        assert_option_subset!(x_axis, x_axis);
        assert_option_subset!(x_axis, origin);
        assert_option_subset!(x_axis, x_point);
        assert_not_option_subset!(x_axis, everything);
        assert_not_option_subset!(x_axis, y_axis);
        assert_not_option_subset!(x_axis, y_point);
        assert_not_option_subset!(x_axis, point);

        assert_option_subset!(origin, origin);
        assert_not_option_subset!(origin, everything);
        assert_not_option_subset!(origin, y_axis);
        assert_not_option_subset!(origin, x_axis);
        assert_not_option_subset!(origin, y_point);
        assert_not_option_subset!(origin, x_point);
        assert_not_option_subset!(origin, point);
    }

    #[test]
    fn point_or_plan_unnamed() {
        #[derive(Debug, OptionSubset)]
        struct PointOrPlane(Option<i64>, Option<i64>);

        let everything = PointOrPlane(None, None);
        let y_axis = PointOrPlane(Some(0), None);
        let x_axis = PointOrPlane(None, Some(0));
        let origin = PointOrPlane(Some(0), Some(0));
        let x_point = PointOrPlane(Some(1), Some(0));
        let y_point = PointOrPlane(Some(0), Some(1));
        let point = PointOrPlane(Some(1), Some(1));

        assert_option_subset!(everything, everything);
        assert_option_subset!(everything, y_axis);
        assert_option_subset!(everything, x_axis);
        assert_option_subset!(everything, origin);
        assert_option_subset!(everything, y_point);
        assert_option_subset!(everything, x_point);
        assert_option_subset!(everything, point);

        assert_option_subset!(y_axis, y_axis);
        assert_option_subset!(y_axis, origin);
        assert_option_subset!(y_axis, y_point);
        assert_not_option_subset!(y_axis, everything);
        assert_not_option_subset!(y_axis, x_axis);
        assert_not_option_subset!(y_axis, x_point);
        assert_not_option_subset!(y_axis, point);

        assert_option_subset!(x_axis, x_axis);
        assert_option_subset!(x_axis, origin);
        assert_option_subset!(x_axis, x_point);
        assert_not_option_subset!(x_axis, everything);
        assert_not_option_subset!(x_axis, y_axis);
        assert_not_option_subset!(x_axis, y_point);
        assert_not_option_subset!(x_axis, point);

        assert_option_subset!(origin, origin);
        assert_not_option_subset!(origin, everything);
        assert_not_option_subset!(origin, y_axis);
        assert_not_option_subset!(origin, x_axis);
        assert_not_option_subset!(origin, y_point);
        assert_not_option_subset!(origin, x_point);
        assert_not_option_subset!(origin, point);
    }

    #[derive(Debug, OptionSubset)]
    enum CompressionType {
        Lzo,
        Lz4,
        Zstd,
    }

    #[test]
    fn compression_type() {
        assert_option_subset!(CompressionType::Lzo, CompressionType::Lzo);
        assert_not_option_subset!(CompressionType::Lzo, CompressionType::Lz4);
        assert_not_option_subset!(CompressionType::Lzo, CompressionType::Zstd);

        assert_option_subset!(CompressionType::Lz4, CompressionType::Lz4);
        assert_not_option_subset!(CompressionType::Lz4, CompressionType::Lzo);
        assert_not_option_subset!(CompressionType::Lz4, CompressionType::Zstd);

        assert_option_subset!(CompressionType::Zstd, CompressionType::Zstd);
        assert_not_option_subset!(CompressionType::Zstd, CompressionType::Lzo);
        assert_not_option_subset!(CompressionType::Zstd, CompressionType::Lz4);
    }

    #[derive(Debug, OptionSubset)]
    struct CompressionData {
        kind: CompressionType,
        level: Option<u64>,
    }

    #[test]
    fn compression_data() {
        let lzo_nl = CompressionData {
            kind: CompressionType::Lzo,
            level: None,
        };
        let lzo_l4 = CompressionData {
            kind: CompressionType::Lzo,
            level: Some(4),
        };
        let lzo_l5 = CompressionData {
            kind: CompressionType::Lzo,
            level: Some(5),
        };
        let lz4_nl = CompressionData {
            kind: CompressionType::Lz4,
            level: None,
        };
        let lz4_l4 = CompressionData {
            kind: CompressionType::Lz4,
            level: Some(4),
        };
        let lz4_l5 = CompressionData {
            kind: CompressionType::Lz4,
            level: Some(5),
        };
        let zstd_nl = CompressionData {
            kind: CompressionType::Zstd,
            level: None,
        };
        let zstd_l4 = CompressionData {
            kind: CompressionType::Zstd,
            level: Some(4),
        };
        let zstd_l5 = CompressionData {
            kind: CompressionType::Zstd,
            level: Some(5),
        };

        assert_option_subset!(lzo_nl, lzo_nl);
        assert_option_subset!(lzo_nl, lzo_l4);
        assert_option_subset!(lzo_nl, lzo_l5);

        assert_not_option_subset!(lzo_l4, lzo_nl);
        assert_option_subset!(lzo_l4, lzo_l4);
        assert_not_option_subset!(lzo_l4, lzo_l5);

        assert_not_option_subset!(lzo_l5, lzo_nl);
        assert_not_option_subset!(lzo_l5, lzo_l4);
        assert_option_subset!(lzo_l5, lzo_l5);

        let lzos = [lzo_nl, lzo_l4, lzo_l5];
        let lz4s = [lz4_nl, lz4_l4, lz4_l5];
        let zstds = [zstd_nl, zstd_l4, zstd_l5];

        assert!(lzos
            .iter()
            .zip(lz4s.iter().chain(zstds.iter()))
            .all(|(lzo, other)| !lzo.option_subset(other)));
        assert!(lz4s
            .iter()
            .zip(lzos.iter().chain(zstds.iter()))
            .all(|(lz4, other)| !lz4.option_subset(other)));
        assert!(zstds
            .iter()
            .zip(lzos.iter().chain(lz4s.iter()))
            .all(|(zstd, other)| !zstd.option_subset(other)));
    }

    #[test]
    fn filter_data() {
        #[derive(Debug, OptionSubset)]
        enum FilterData {
            Compression(CompressionData),
            ScaleFloat {
                byte_width: u64,
                factor: Option<f64>,
                offset: Option<f64>,
            },
            Xor,
        }

        let sf_none = FilterData::ScaleFloat {
            byte_width: 0,
            factor: None,
            offset: None,
        };
        let sf_factor = FilterData::ScaleFloat {
            byte_width: 0,
            factor: Some(1.0),
            offset: None,
        };
        let sf_offset = FilterData::ScaleFloat {
            byte_width: 0,
            factor: None,
            offset: Some(1.0),
        };
        let sf_all = FilterData::ScaleFloat {
            byte_width: 0,
            factor: Some(1.0),
            offset: Some(1.0),
        };

        assert_option_subset!(sf_none, sf_none);
        assert_option_subset!(sf_none, sf_factor);
        assert_option_subset!(sf_none, sf_offset);
        assert_option_subset!(sf_none, sf_all);

        assert_not_option_subset!(sf_factor, sf_none);
        assert_option_subset!(sf_factor, sf_factor);
        assert_not_option_subset!(sf_factor, sf_offset);
        assert_option_subset!(sf_factor, sf_all);

        assert_not_option_subset!(sf_offset, sf_none);
        assert_not_option_subset!(sf_offset, sf_factor);
        assert_option_subset!(sf_offset, sf_offset);
        assert_option_subset!(sf_offset, sf_all);

        assert_not_option_subset!(sf_all, sf_none);
        assert_not_option_subset!(sf_all, sf_factor);
        assert_not_option_subset!(sf_all, sf_offset);
        assert_option_subset!(sf_all, sf_all);

        let sf_bw1 = FilterData::ScaleFloat {
            byte_width: 1,
            factor: None,
            offset: None,
        };

        assert_not_option_subset!(sf_none, sf_bw1);
        assert_not_option_subset!(sf_factor, sf_bw1);
        assert_not_option_subset!(sf_offset, sf_bw1);
        assert_not_option_subset!(sf_all, sf_bw1);

        assert_option_subset!(FilterData::Xor, FilterData::Xor);

        assert_not_option_subset!(sf_none, FilterData::Xor);
        assert_not_option_subset!(sf_factor, FilterData::Xor);
        assert_not_option_subset!(sf_offset, FilterData::Xor);
        assert_not_option_subset!(sf_all, FilterData::Xor);
        assert_not_option_subset!(FilterData::Xor, sf_none);
        assert_not_option_subset!(FilterData::Xor, sf_factor);
        assert_not_option_subset!(FilterData::Xor, sf_offset);
        assert_not_option_subset!(FilterData::Xor, sf_all);

        let lzo_nl = FilterData::Compression(CompressionData {
            kind: CompressionType::Lzo,
            level: None,
        });
        let lzo_l4 = FilterData::Compression(CompressionData {
            kind: CompressionType::Lzo,
            level: Some(4),
        });
        let lzo_l5 = FilterData::Compression(CompressionData {
            kind: CompressionType::Lzo,
            level: Some(5),
        });
        let lz4_nl = FilterData::Compression(CompressionData {
            kind: CompressionType::Lz4,
            level: None,
        });
        let lz4_l4 = FilterData::Compression(CompressionData {
            kind: CompressionType::Lz4,
            level: Some(4),
        });
        let lz4_l5 = FilterData::Compression(CompressionData {
            kind: CompressionType::Lz4,
            level: Some(5),
        });
        let zstd_nl = FilterData::Compression(CompressionData {
            kind: CompressionType::Zstd,
            level: None,
        });
        let zstd_l4 = FilterData::Compression(CompressionData {
            kind: CompressionType::Zstd,
            level: Some(4),
        });
        let zstd_l5 = FilterData::Compression(CompressionData {
            kind: CompressionType::Zstd,
            level: Some(5),
        });

        assert_option_subset!(lzo_nl, lzo_nl);
        assert_option_subset!(lzo_nl, lzo_l4);
        assert_option_subset!(lzo_nl, lzo_l5);

        assert_not_option_subset!(lzo_l4, lzo_nl);
        assert_option_subset!(lzo_l4, lzo_l4);
        assert_not_option_subset!(lzo_l4, lzo_l5);

        assert_not_option_subset!(lzo_l5, lzo_nl);
        assert_not_option_subset!(lzo_l5, lzo_l4);
        assert_option_subset!(lzo_l5, lzo_l5);

        let lzos = [lzo_nl, lzo_l4, lzo_l5];
        let lz4s = [lz4_nl, lz4_l4, lz4_l5];
        let zstds = [zstd_nl, zstd_l4, zstd_l5];
        let sfs = [sf_none, sf_factor, sf_offset, sf_all];
        let xors = [FilterData::Xor];

        assert!(lzos
            .iter()
            .zip(
                lz4s.iter()
                    .chain(zstds.iter())
                    .chain(sfs.iter())
                    .chain(xors.iter())
            )
            .all(|(lzo, other)| !lzo.option_subset(other)));
        assert!(lz4s
            .iter()
            .zip(
                lzos.iter()
                    .chain(zstds.iter())
                    .chain(sfs.iter())
                    .chain(xors.iter())
            )
            .all(|(lz4, other)| !lz4.option_subset(other)));
        assert!(zstds
            .iter()
            .zip(
                lzos.iter()
                    .chain(lz4s.iter())
                    .chain(sfs.iter())
                    .chain(xors.iter())
            )
            .all(|(zstd, other)| !zstd.option_subset(other)));
        assert!(sfs
            .iter()
            .zip(
                lzos.iter()
                    .chain(lz4s.iter())
                    .chain(zstds.iter())
                    .chain(xors.iter())
            )
            .all(|(sf, other)| !sf.option_subset(other)));
        assert!(xors
            .iter()
            .zip(
                lzos.iter()
                    .chain(lz4s.iter())
                    .chain(zstds.iter())
                    .chain(sfs.iter())
            )
            .all(|(xor, other)| !xor.option_subset(other)));
    }

    #[cfg(feature = "serde_json")]
    mod serde_json {
        use super::*;
        use crate::serde_json::json;
        use crate::serde_json::value::{Map, Value};

        #[test]
        fn map() {
            let m_empty = Map::new();

            let m_nullval = {
                let mut m = Map::new();
                m.insert("k".to_string(), Value::Null);
                m
            };
            let m_someval = {
                let mut m = Map::new();
                m.insert("k".to_string(), json!(1));
                m
            };
            let m_mapval = {
                let mut m = Map::new();
                m.insert("k".to_string(), json!(Map::new()));
                m
            };
            let m_nested_nullval = {
                let mut subm = Map::new();
                subm.insert("subk".to_string(), Value::Null);
                let mut m = Map::new();
                m.insert("k".to_string(), Value::Object(subm));
                m
            };
            let m_nested_someval = {
                let mut subm = Map::new();
                subm.insert("subk".to_string(), json!("gub"));
                let mut m = Map::new();
                m.insert("k".to_string(), Value::Object(subm));
                m
            };

            let m_extrakey = {
                let mut m = Map::new();
                m.insert("k".to_string(), Value::Null);
                m.insert("z".to_string(), Value::Null);
                m
            };

            assert_option_subset!(m_empty, m_empty);
            assert_option_subset!(m_empty, m_nullval);
            assert_option_subset!(m_empty, m_someval);
            assert_option_subset!(m_empty, m_mapval);
            assert_option_subset!(m_empty, m_nested_nullval);
            assert_option_subset!(m_empty, m_nested_someval);

            assert_option_subset!(m_nullval, m_someval);
            assert_option_subset!(m_nullval, m_mapval);
            assert_option_subset!(m_nullval, m_nested_nullval);
            assert_option_subset!(m_nullval, m_nested_someval);

            assert_option_subset!(m_mapval, m_nested_nullval);
            assert_option_subset!(m_mapval, m_nested_someval);

            assert_not_option_subset!(m_nullval, m_empty);
            assert_not_option_subset!(m_someval, m_nullval);
            assert_not_option_subset!(m_mapval, m_nullval);
            assert_not_option_subset!(m_nested_nullval, m_mapval);
            assert_not_option_subset!(m_nested_someval, m_nested_nullval);

            // type mismatch
            assert_not_option_subset!(m_someval, m_mapval);

            // key present in left but not right
            assert_option_subset!(m_empty, m_extrakey);
            assert_option_subset!(m_nullval, m_extrakey);
            assert_not_option_subset!(m_extrakey, m_empty);
            assert_not_option_subset!(m_extrakey, m_nullval);
        }

        #[test]
        fn serde_json_value() {
            let j_null = Value::Null;
            let j_true = Value::Bool(true);
            let j_false = Value::Bool(false);
            let j_zero = json!(0);
            let j_one = json!(1);
            let j_hello = json!("hello");
            let j_world = json!("world");

            let j_arrnullelt0 = Value::Array(vec![Value::Null, json!(1)]);
            let j_arrnullelt1 = Value::Array(vec![json!(0), Value::Null]);
            let j_arrnonnull = Value::Array(vec![json!(0), json!(1)]);
            let j_arrnoteq = Value::Array(vec![json!(1), json!(2)]);

            let j_empty = Value::Object(Map::<String, Value>::new());

            assert_option_subset!(j_null, j_null);
            assert_option_subset!(j_null, j_true);
            assert_option_subset!(j_null, j_false);
            assert_option_subset!(j_null, j_zero);
            assert_option_subset!(j_null, j_one);
            assert_option_subset!(j_null, j_hello);
            assert_option_subset!(j_null, j_arrnullelt0);
            assert_option_subset!(j_null, j_arrnullelt1);
            assert_option_subset!(j_null, j_arrnonnull);
            assert_option_subset!(j_null, j_arrnoteq);
            assert_option_subset!(j_null, j_empty);

            assert_option_subset!(j_true, j_true);
            assert_not_option_subset!(j_true, j_null);
            assert_not_option_subset!(j_true, j_false);
            assert_not_option_subset!(j_true, j_zero);
            assert_not_option_subset!(j_true, j_hello);
            assert_not_option_subset!(j_true, j_arrnullelt0);
            assert_not_option_subset!(j_true, j_empty);

            assert_option_subset!(j_zero, j_zero);
            assert_not_option_subset!(j_zero, j_null);
            assert_not_option_subset!(j_zero, j_false);
            assert_not_option_subset!(j_zero, j_one);
            assert_not_option_subset!(j_zero, j_hello);
            assert_not_option_subset!(j_zero, j_arrnullelt0);
            assert_not_option_subset!(j_zero, j_empty);

            assert_option_subset!(j_hello, j_hello);
            assert_not_option_subset!(j_hello, j_null);
            assert_not_option_subset!(j_hello, j_false);
            assert_not_option_subset!(j_hello, j_one);
            assert_not_option_subset!(j_hello, j_world);
            assert_not_option_subset!(j_hello, j_arrnullelt0);
            assert_not_option_subset!(j_hello, j_empty);

            assert_option_subset!(j_arrnullelt0, j_arrnullelt0);
            assert_option_subset!(j_arrnullelt0, j_arrnonnull);
            assert_not_option_subset!(j_arrnullelt0, j_null);
            assert_not_option_subset!(j_arrnullelt0, j_true);
            assert_not_option_subset!(j_arrnullelt0, j_zero);
            assert_not_option_subset!(j_arrnullelt0, j_arrnullelt1);
            assert_not_option_subset!(j_arrnullelt0, j_arrnoteq);
            assert_not_option_subset!(j_arrnullelt0, j_empty);

            assert_not_option_subset!(j_empty, j_null);
            assert_not_option_subset!(j_empty, j_null);
            assert_not_option_subset!(j_empty, j_false);
            assert_not_option_subset!(j_empty, j_one);
            assert_not_option_subset!(j_empty, j_hello);
            assert_not_option_subset!(j_empty, j_arrnullelt0);
        }
    }
}
