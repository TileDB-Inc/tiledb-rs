#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ArrowConversionResult<I, E> {
    None,
    Inexact(I),
    Exact(E),
}

impl<T> ArrowConversionResult<T, T> {
    pub fn is_inexact(&self) -> bool {
        matches!(self, Self::Inexact(_))
    }

    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact(_))
    }

    pub fn ok(self) -> Option<T> {
        match self {
            Self::None => None,
            Self::Inexact(t) => Some(t),
            Self::Exact(t) => Some(t),
        }
    }
}
