mod compatibility;
mod default_to;

pub enum TypeConversion<Output> {
    PhysicalMatch(Output),
    LogicalMatch(Output),
}

impl<Output> TypeConversion<Output> {
    pub fn into_inner(self) -> Output {
        match self {
            Self::PhysicalMatch(dt) => dt,
            Self::LogicalMatch(dt) => dt,
        }
    }
}

pub use self::compatibility::is_physically_compatible;
pub use self::default_to::{
    default_arrow_type, NoMatchDetail as DefaultArrowTypeError,
};

#[cfg(test)]
mod tests {
    use tiledb_common::array::CellValNum;
    use tiledb_common::Datatype;

    use super::*;

    #[test]
    fn default_compatibility() {
        for dt in Datatype::iter() {
            let single = default_arrow_type(dt, CellValNum::single())
                .unwrap()
                .into_inner();
            assert!(
                is_physically_compatible(&single, dt, CellValNum::single()),
                "arrow = {}, tiledb = {}",
                single,
                dt
            );

            let fixed_cvn = CellValNum::try_from(4).unwrap();
            let fixed = default_arrow_type(dt, fixed_cvn).unwrap().into_inner();
            assert!(
                is_physically_compatible(&fixed, dt, fixed_cvn),
                "arrow = {}, tiledb = {}",
                fixed,
                dt
            );

            let var = default_arrow_type(dt, CellValNum::Var)
                .unwrap()
                .into_inner();
            assert!(
                is_physically_compatible(&var, dt, CellValNum::Var),
                "arrow = {}, tiledb = {}",
                var,
                dt
            );
        }
    }
}
