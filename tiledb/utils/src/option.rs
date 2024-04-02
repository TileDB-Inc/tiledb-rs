/// Trait for comparing types which can express optional data.
/// The `option_subset` method should return true if it finds that all required data
/// of two objects are equal, and the non-required data are either equal or not set
/// for the method receiver.  For objects which only have required data, this should be
/// the same as `eq`.
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

impl<T> OptionSubset for Vec<T>
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

option_subset_partialeq!(u8, u16, u32, u64, usize);
option_subset_partialeq!(i8, i16, i32, i64, isize);
option_subset_partialeq!(bool, f32, f64, String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit() {
        #[derive(OptionSubset)]
        struct Unit;

        assert!(Unit.option_subset(&Unit));
    }

    #[test]
    fn point_unnamed() {
        #[derive(OptionSubset)]
        struct PointUnnamed(i64, i64);

        assert!(PointUnnamed(0, 0).option_subset(&PointUnnamed(0, 0)));
        assert!(!PointUnnamed(0, 0).option_subset(&PointUnnamed(0, 1)));
    }

    #[derive(OptionSubset)]
    struct PointNamed {
        x: i64,
        y: i64,
    }

    #[test]
    fn point_named() {
        impl PointNamed {
            pub fn new(x: i64, y: i64) -> Self {
                PointNamed { x, y }
            }
        }

        assert!(PointNamed::new(0, 0).option_subset(&PointNamed::new(0, 0)));
        assert!(!PointNamed::new(0, 0).option_subset(&PointNamed::new(0, 1)));
    }

    #[test]
    fn point_kd_named() {
        #[derive(OptionSubset)]
        struct PointKDNamed {
            axes: Vec<i64>,
        }

        impl PointKDNamed {
            pub fn new(axes: Vec<i64>) -> Self {
                PointKDNamed { axes }
            }
        }

        assert!(
            PointKDNamed::new(vec![]).option_subset(&PointKDNamed::new(vec![]))
        );
        assert!(PointKDNamed::new(vec![0])
            .option_subset(&PointKDNamed::new(vec![0])));
        assert!(PointKDNamed::new(vec![0, 0])
            .option_subset(&PointKDNamed::new(vec![0, 0])));
        assert!(!PointKDNamed::new(vec![0, 0])
            .option_subset(&PointKDNamed::new(vec![0])));
        assert!(!PointKDNamed::new(vec![0, 0])
            .option_subset(&PointKDNamed::new(vec![0, 0, 0])));
        assert!(!PointKDNamed::new(vec![0, 0])
            .option_subset(&PointKDNamed::new(vec![0, 1])));
    }

    #[test]
    fn point_kd_unnamed() {
        #[derive(OptionSubset)]
        struct PointKDUnnamed(Vec<i64>);

        assert!(PointKDUnnamed(vec![]).option_subset(&PointKDUnnamed(vec![])));
        assert!(PointKDUnnamed(vec![0]).option_subset(&PointKDUnnamed(vec![0])));
        assert!(PointKDUnnamed(vec![0, 0])
            .option_subset(&PointKDUnnamed(vec![0, 0])));
        assert!(
            !PointKDUnnamed(vec![0, 0]).option_subset(&PointKDUnnamed(vec![0]))
        );
        assert!(!PointKDUnnamed(vec![0, 0])
            .option_subset(&PointKDUnnamed(vec![0, 0, 0])));
        assert!(!PointKDUnnamed(vec![0, 0])
            .option_subset(&PointKDUnnamed(vec![0, 1])));
    }

    #[test]
    fn point_or_plane_named() {
        #[derive(OptionSubset)]
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

        assert!(everything.option_subset(&everything));
        assert!(everything.option_subset(&y_axis));
        assert!(everything.option_subset(&x_axis));
        assert!(everything.option_subset(&origin));
        assert!(everything.option_subset(&y_point));
        assert!(everything.option_subset(&x_point));
        assert!(everything.option_subset(&point));

        assert!(y_axis.option_subset(&y_axis));
        assert!(y_axis.option_subset(&origin));
        assert!(y_axis.option_subset(&y_point));
        assert!(!y_axis.option_subset(&everything));
        assert!(!y_axis.option_subset(&x_axis));
        assert!(!y_axis.option_subset(&x_point));
        assert!(!y_axis.option_subset(&point));

        assert!(x_axis.option_subset(&x_axis));
        assert!(x_axis.option_subset(&origin));
        assert!(x_axis.option_subset(&x_point));
        assert!(!x_axis.option_subset(&everything));
        assert!(!x_axis.option_subset(&y_axis));
        assert!(!x_axis.option_subset(&y_point));
        assert!(!x_axis.option_subset(&point));

        assert!(origin.option_subset(&origin));
        assert!(!origin.option_subset(&everything));
        assert!(!origin.option_subset(&y_axis));
        assert!(!origin.option_subset(&x_axis));
        assert!(!origin.option_subset(&y_point));
        assert!(!origin.option_subset(&x_point));
        assert!(!origin.option_subset(&point));
    }

    #[test]
    fn point_or_plan_unnamed() {
        #[derive(OptionSubset)]
        struct PointOrPlane(Option<i64>, Option<i64>);

        let everything = PointOrPlane(None, None);
        let y_axis = PointOrPlane(Some(0), None);
        let x_axis = PointOrPlane(None, Some(0));
        let origin = PointOrPlane(Some(0), Some(0));
        let x_point = PointOrPlane(Some(1), Some(0));
        let y_point = PointOrPlane(Some(0), Some(1));
        let point = PointOrPlane(Some(1), Some(1));

        assert!(everything.option_subset(&everything));
        assert!(everything.option_subset(&y_axis));
        assert!(everything.option_subset(&x_axis));
        assert!(everything.option_subset(&origin));
        assert!(everything.option_subset(&y_point));
        assert!(everything.option_subset(&x_point));
        assert!(everything.option_subset(&point));

        assert!(y_axis.option_subset(&y_axis));
        assert!(y_axis.option_subset(&origin));
        assert!(y_axis.option_subset(&y_point));
        assert!(!y_axis.option_subset(&everything));
        assert!(!y_axis.option_subset(&x_axis));
        assert!(!y_axis.option_subset(&x_point));
        assert!(!y_axis.option_subset(&point));

        assert!(x_axis.option_subset(&x_axis));
        assert!(x_axis.option_subset(&origin));
        assert!(x_axis.option_subset(&x_point));
        assert!(!x_axis.option_subset(&everything));
        assert!(!x_axis.option_subset(&y_axis));
        assert!(!x_axis.option_subset(&y_point));
        assert!(!x_axis.option_subset(&point));

        assert!(origin.option_subset(&origin));
        assert!(!origin.option_subset(&everything));
        assert!(!origin.option_subset(&y_axis));
        assert!(!origin.option_subset(&x_axis));
        assert!(!origin.option_subset(&y_point));
        assert!(!origin.option_subset(&x_point));
        assert!(!origin.option_subset(&point));
    }

    #[derive(OptionSubset)]
    enum CompressionType {
        Lzo,
        Lz4,
        Zstd,
    }

    #[test]
    fn compression_type() {
        assert!(CompressionType::Lzo.option_subset(&CompressionType::Lzo));
        assert!(!CompressionType::Lzo.option_subset(&CompressionType::Lz4));
        assert!(!CompressionType::Lzo.option_subset(&CompressionType::Zstd));

        assert!(CompressionType::Lz4.option_subset(&CompressionType::Lz4));
        assert!(!CompressionType::Lz4.option_subset(&CompressionType::Lzo));
        assert!(!CompressionType::Lz4.option_subset(&CompressionType::Zstd));

        assert!(CompressionType::Zstd.option_subset(&CompressionType::Zstd));
        assert!(!CompressionType::Zstd.option_subset(&CompressionType::Lzo));
        assert!(!CompressionType::Zstd.option_subset(&CompressionType::Lz4));
    }

    #[derive(OptionSubset)]
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

        assert!(lzo_nl.option_subset(&lzo_nl));
        assert!(lzo_nl.option_subset(&lzo_l4));
        assert!(lzo_nl.option_subset(&lzo_l5));

        assert!(!lzo_l4.option_subset(&lzo_nl));
        assert!(lzo_l4.option_subset(&lzo_l4));
        assert!(!lzo_l4.option_subset(&lzo_l5));

        assert!(!lzo_l5.option_subset(&lzo_nl));
        assert!(!lzo_l5.option_subset(&lzo_l4));
        assert!(lzo_l5.option_subset(&lzo_l5));

        let lzos = vec![lzo_nl, lzo_l4, lzo_l5];
        let lz4s = vec![lz4_nl, lz4_l4, lz4_l5];
        let zstds = vec![zstd_nl, zstd_l4, zstd_l5];

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
        #[derive(OptionSubset)]
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

        assert!(sf_none.option_subset(&sf_none));
        assert!(sf_none.option_subset(&sf_factor));
        assert!(sf_none.option_subset(&sf_offset));
        assert!(sf_none.option_subset(&sf_all));

        assert!(!sf_factor.option_subset(&sf_none));
        assert!(sf_factor.option_subset(&sf_factor));
        assert!(!sf_factor.option_subset(&sf_offset));
        assert!(sf_factor.option_subset(&sf_all));

        assert!(!sf_offset.option_subset(&sf_none));
        assert!(!sf_offset.option_subset(&sf_factor));
        assert!(sf_offset.option_subset(&sf_offset));
        assert!(sf_offset.option_subset(&sf_all));

        assert!(!sf_all.option_subset(&sf_none));
        assert!(!sf_all.option_subset(&sf_factor));
        assert!(!sf_all.option_subset(&sf_offset));
        assert!(sf_all.option_subset(&sf_all));

        let sf_bw1 = FilterData::ScaleFloat {
            byte_width: 1,
            factor: None,
            offset: None,
        };

        assert!(!sf_none.option_subset(&sf_bw1));
        assert!(!sf_factor.option_subset(&sf_bw1));
        assert!(!sf_offset.option_subset(&sf_bw1));
        assert!(!sf_all.option_subset(&sf_bw1));

        assert!(FilterData::Xor.option_subset(&FilterData::Xor));

        assert!(!sf_none.option_subset(&FilterData::Xor));
        assert!(!sf_factor.option_subset(&FilterData::Xor));
        assert!(!sf_offset.option_subset(&FilterData::Xor));
        assert!(!sf_all.option_subset(&FilterData::Xor));
        assert!(!FilterData::Xor.option_subset(&sf_none));
        assert!(!FilterData::Xor.option_subset(&sf_factor));
        assert!(!FilterData::Xor.option_subset(&sf_offset));
        assert!(!FilterData::Xor.option_subset(&sf_all));

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

        assert!(lzo_nl.option_subset(&lzo_nl));
        assert!(lzo_nl.option_subset(&lzo_l4));
        assert!(lzo_nl.option_subset(&lzo_l5));

        assert!(!lzo_l4.option_subset(&lzo_nl));
        assert!(lzo_l4.option_subset(&lzo_l4));
        assert!(!lzo_l4.option_subset(&lzo_l5));

        assert!(!lzo_l5.option_subset(&lzo_nl));
        assert!(!lzo_l5.option_subset(&lzo_l4));
        assert!(lzo_l5.option_subset(&lzo_l5));

        let lzos = vec![lzo_nl, lzo_l4, lzo_l5];
        let lz4s = vec![lz4_nl, lz4_l4, lz4_l5];
        let zstds = vec![zstd_nl, zstd_l4, zstd_l5];
        let sfs = vec![sf_none, sf_factor, sf_offset, sf_all];
        let xors = vec![FilterData::Xor];

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
}
