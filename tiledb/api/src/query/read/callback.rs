use super::*;

use anyhow::anyhow;
use itertools::izip;
use paste::paste;

use crate::query::buffer::RefTypedQueryBuffersMut;
use crate::query::read::output::{
    FromQueryOutput, RawReadOutput, TypedRawReadOutput,
};

macro_rules! trait_read_callback {
    ($name:ident, $($U:ident),+) => {
        pub trait $name: Sized {
            $(
                type $U: CellValue;
            )+
            type Intermediate;
            type Final;
            type Error: Into<anyhow::Error>;

            paste! {
                fn intermediate_result(
                    &mut self,
                    $(
                        [< arg_ $U:snake >]: RawReadOutput<Self::$U>
                    ),+
                ) -> Result<Self::Intermediate, Self::Error>;

                fn final_result(
                    self,
                    $(
                        [< arg_ $U:snake >]: RawReadOutput<Self::$U>
                    ),+
                ) -> Result<Self::Final, Self::Error>;
            }

            /// Optionally produce a blank instance of this callback to be run
            /// if the query is restarted from the beginning. This is called
            /// before `final_result` to prepare the query for re-submission
            /// if necessary.
            fn cleared(&self) -> Option<Self> {
                None
            }
        }
    };
}

trait_read_callback!(ReadCallback, Unit);
trait_read_callback!(ReadCallback2Arg, Unit1, Unit2);
trait_read_callback!(ReadCallback3Arg, Unit1, Unit2, Unit3);
trait_read_callback!(ReadCallback4Arg, Unit1, Unit2, Unit3, Unit4);

pub trait ReadCallbackVarArg: Sized {
    type Intermediate;
    type Final;
    type Error: Into<anyhow::Error>;

    /* TODO: some kind of argument validation */

    fn intermediate_result(
        &mut self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Intermediate, Self::Error>;

    fn final_result(
        self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Final, Self::Error>;

    /// Optionally produce a blank instance of this callback to be run
    /// if the query is restarted from the beginning. This is called
    /// before `final_result` to prepare the query for re-submission
    /// if necessary.
    fn cleared(&self) -> Option<Self> {
        None
    }
}

#[derive(Clone)]
pub struct FnMutAdapter<A, F> {
    arg: std::marker::PhantomData<A>,
    func: F,
}

impl<A, F> FnMutAdapter<A, F> {
    pub fn new(func: F) -> Self {
        FnMutAdapter {
            arg: std::marker::PhantomData,
            func,
        }
    }
}

impl<A, F> ReadCallback for FnMutAdapter<A, F>
where
    A: FromQueryOutput,
    <A as FromQueryOutput>::Unit: CellValue,
    F: Clone + FnMut(A),
{
    type Unit = <A as FromQueryOutput>::Unit;
    type Intermediate = ();
    type Final = ();
    type Error = crate::error::Error;

    fn intermediate_result(
        &mut self,
        arg: RawReadOutput<Self::Unit>,
    ) -> Result<Self::Intermediate, Self::Error> {
        let iter = <A as FromQueryOutput>::Iterator::try_from(arg)?;

        for record in iter {
            (self.func)(record)
        }

        Ok(())
    }

    fn final_result(
        mut self,
        arg: RawReadOutput<Self::Unit>,
    ) -> Result<Self::Intermediate, Self::Error> {
        let iter = <A as FromQueryOutput>::Iterator::try_from(arg)?;

        for record in iter {
            (self.func)(record)
        }

        Ok(())
    }

    fn cleared(&self) -> Option<Self> {
        Some(FnMutAdapter {
            arg: self.arg,
            func: self.func.clone(),
        })
    }
}

macro_rules! fn_mut_adapter_tuple {
    ($callback:ident, $($A:ident: $U:ident),+) => {
        impl<$($A),+, F> $callback for FnMutAdapter<($($A),+), F>
        where $(
                $A: FromQueryOutput,
                <$A as FromQueryOutput>::Unit: CellValue
            ),+,
            F: Clone + FnMut($($A),+)
        {
            $(
                type $U = <$A as FromQueryOutput>::Unit;
            )+
            type Intermediate = ();
            type Final = ();
            type Error = crate::error::Error;

            paste! {
                fn intermediate_result(
                    &mut self,
                    $(
                        [< $A:snake >]: RawReadOutput<Self::$U>
                    ),+
                ) -> Result<Self::Intermediate, Self::Error>
                {
                    $(
                        let [< iter_ $A:snake >] = <$A as FromQueryOutput>::Iterator::try_from(
                            [< $A:snake >],
                        )?;
                    )+

                    for ($([< r_ $A:snake >]),+) in izip!($([< iter_ $A:snake >]),+) {
                        (self.func)($([< r_ $A:snake >]),+)
                    }

                    Ok(())
                }

                fn final_result(
                    mut self,
                    $(
                        [< $A:snake >]: RawReadOutput<Self::$U>
                    ),+
                ) -> Result<Self::Final, Self::Error>
                {
                    $(
                        let [< iter_ $A:snake >] = <$A as FromQueryOutput>::Iterator::try_from(
                            [< $A:snake >],
                        )?;
                    )+

                    for ($([< r_ $A:snake >]),+) in izip!($([< iter_ $A:snake >]),+) {
                        (self.func)($([< r_ $A:snake >]),+)
                    }

                    Ok(())
                }
            }

            fn cleared(&self) -> Option<Self> {
                Some(FnMutAdapter {
                    arg: self.arg,
                    func: self.func.clone(),
                })
            }
        }
    };
}

fn_mut_adapter_tuple!(ReadCallback2Arg, A1: Unit1, A2: Unit2);
fn_mut_adapter_tuple!(ReadCallback3Arg, A1: Unit1, A2: Unit2, A3: Unit3);
fn_mut_adapter_tuple!(ReadCallback4Arg, A1: Unit1, A2: Unit2, A3: Unit3, A4: Unit4);

pub struct MapIntermediate<S, F> {
    callback: S,
    transform: F,
}

impl<S, F, O> ReadCallbackVarArg for MapIntermediate<S, F>
where
    S: ReadCallbackVarArg,
    F: FnMut(<S as ReadCallbackVarArg>::Intermediate) -> O,
{
    type Intermediate = O;
    type Final = <S as ReadCallbackVarArg>::Final;
    type Error = <S as ReadCallbackVarArg>::Error;

    fn intermediate_result(
        &mut self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Intermediate, Self::Error> {
        Ok((self.transform)(self.callback.intermediate_result(args)?))
    }

    fn final_result(
        self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Final, Self::Error> {
        self.callback.final_result(args)
    }
}

pub struct MapFinal<S, F> {
    callback: S,
    transform: F,
}

impl<S, F, O> ReadCallbackVarArg for MapFinal<S, F>
where
    S: ReadCallbackVarArg,
    F: FnMut(<S as ReadCallbackVarArg>::Final) -> O,
{
    type Intermediate = <S as ReadCallbackVarArg>::Intermediate;
    type Final = O;
    type Error = <S as ReadCallbackVarArg>::Error;

    fn intermediate_result(
        &mut self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Intermediate, Self::Error> {
        self.callback.intermediate_result(args)
    }

    fn final_result(
        mut self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Final, Self::Error> {
        Ok((self.transform)(self.callback.final_result(args)?))
    }
}

pub trait Map<I, F>: Sized {
    type Intermediate;
    type Final;

    fn map_intermediate(&mut self, input: I) -> Self::Intermediate;
    fn map_final(self, input: F) -> Self::Final;
}

pub struct MapAdapter<M, S> {
    transform: M,
    callback: S,
}

impl<M, S> MapAdapter<M, S> {
    fn new(transform: M, callback: S) -> Self {
        MapAdapter {
            transform,
            callback,
        }
    }
}

impl<M, S> ReadCallbackVarArg for MapAdapter<M, S>
where
    S: ReadCallbackVarArg,
    M: Map<S::Intermediate, S::Final>,
{
    type Intermediate = M::Intermediate;
    type Final = M::Final;
    type Error = S::Error;

    fn intermediate_result(
        &mut self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Intermediate, Self::Error> {
        Ok(self
            .transform
            .map_intermediate(self.callback.intermediate_result(args)?))
    }

    fn final_result(
        self,
        args: Vec<TypedRawReadOutput>,
    ) -> Result<Self::Final, Self::Error> {
        Ok(self.transform.map_final(self.callback.final_result(args)?))
    }
}

macro_rules! map_impls {
    ($callback:ident, $($A:ident: $U:ident),+) => {
        impl<S, F, O> $callback for MapIntermediate<S, F> where S: $callback, F: FnMut(<S as $callback>::Intermediate) -> O {
            $(
                type $U = <S as $callback>::$U;
            )+
            type Intermediate = O;
            type Final = <S as $callback>::Final;
            type Error = <S as $callback>::Error;

            paste! {
                fn intermediate_result(
                    &mut self,
                    $(
                        [< $A:snake >]: RawReadOutput<Self::$U>
                    ),+
                ) -> Result<Self::Intermediate, Self::Error>
                {
                    Ok((self.transform)(self.callback.intermediate_result($([< $A:snake >]),+)?))
                }

                fn final_result(
                    self,
                    $(
                        [< $A:snake >]: RawReadOutput<Self::$U>
                    ),+
                ) -> Result<Self::Final, Self::Error>
                {
                    self.callback.final_result($([< $A:snake >]),+)
                }
            }
        }

        impl<S, F, O> $callback for MapFinal<S, F> where S: $callback, F: FnMut(<S as $callback>::Final) -> O {
            $(
                type $U = <S as $callback>::$U;
            )+
            type Intermediate = <S as $callback>::Intermediate;
            type Final = O;
            type Error = <S as $callback>::Error;

            paste! {
                fn intermediate_result(
                    &mut self,
                    $(
                        [< $A:snake >]: RawReadOutput<Self::$U>
                    ),+
                ) -> Result<Self::Intermediate, Self::Error>
                {
                    self.callback.intermediate_result($([< $A:snake >]),+)
                }

                fn final_result(
                    mut self,
                    $(
                        [< $A:snake >]: RawReadOutput<Self::$U>
                    ),+
                ) -> Result<Self::Final, Self::Error>
                {
                    Ok((self.transform)(self.callback.final_result($([< $A:snake >]),+)?))
                }
            }
        }

        impl<M, S> $callback for MapAdapter<M, S>
        where S: $callback,
              M: Map<S::Intermediate, S::Final>
        {
            $(
                type $U = <S as $callback>::$U;
            )+
            type Intermediate = M::Intermediate;
            type Final = M::Final;
            type Error = S::Error;

            paste! {
                fn intermediate_result(
                    &mut self,
                    $(
                        [< $A:snake >]: RawReadOutput<Self::$U>
                    ),+
                ) -> Result<Self::Intermediate, Self::Error>
                {
                    Ok(self.transform.map_intermediate(self.callback.intermediate_result($([< $A:snake >]),+)?))
                }

                fn final_result(
                    self,
                    $(
                        [< $A:snake >]: RawReadOutput<Self::$U>
                    ),+
                ) -> Result<Self::Final, Self::Error>
                {
                    Ok(self.transform.map_final(self.callback.final_result($([< $A:snake >]),+)?))
                }
            }

        }
    };
}

map_impls!(ReadCallback, A1: Unit);
map_impls!(ReadCallback2Arg, A1: Unit1, A2: Unit2);
map_impls!(ReadCallback3Arg, A1: Unit1, A2: Unit2, A3: Unit3);
map_impls!(ReadCallback4Arg, A1: Unit1, A2: Unit2, A3: Unit3, A4: Unit4);

mod impls {
    use super::*;
    use crate::query::read::output::VarDataIterator;

    impl<C> ReadCallback for Vec<C>
    where
        C: CellValue,
    {
        type Unit = C;
        type Intermediate = ();
        type Final = Self;
        type Error = std::convert::Infallible;

        fn intermediate_result(
            &mut self,
            arg: RawReadOutput<Self::Unit>,
        ) -> Result<Self::Intermediate, Self::Error> {
            self.extend_from_slice(&arg.input.data.as_ref()[0..arg.nvalues()]);
            Ok(())
        }

        fn final_result(
            mut self,
            arg: RawReadOutput<Self::Unit>,
        ) -> Result<Self::Final, Self::Error> {
            self.intermediate_result(arg).map(|_| self)
        }
    }

    impl<C> ReadCallback for (Vec<C>, Vec<u8>)
    where
        C: CellValue,
    {
        type Unit = C;
        type Intermediate = ();
        type Final = Self;
        type Error = std::convert::Infallible;

        fn intermediate_result(
            &mut self,
            arg: RawReadOutput<Self::Unit>,
        ) -> Result<Self::Intermediate, Self::Error> {
            self.0
                .extend_from_slice(&arg.input.data.as_ref()[0..arg.nvalues()]);
            // TileDB Core currently ensures that all buffers are properly set
            // as required. Thus, this unwrap should never fail as its only
            // called after submit has returned successfully.
            self.1.extend_from_slice(
                &arg.input.validity.as_ref().unwrap().as_ref()[0..arg.ncells],
            );
            Ok(())
        }

        fn final_result(
            mut self,
            arg: RawReadOutput<Self::Unit>,
        ) -> Result<Self::Final, Self::Error> {
            self.intermediate_result(arg).map(|_| self)
        }
    }

    impl<C> ReadCallback for Vec<Vec<C>>
    where
        C: CellValue,
    {
        type Unit = C;
        type Intermediate = ();
        type Final = Self;
        type Error = std::convert::Infallible;

        fn intermediate_result(
            &mut self,
            arg: RawReadOutput<Self::Unit>,
        ) -> Result<Self::Intermediate, Self::Error> {
            // TileDB Core currently ensures that all buffers are properly set
            // as required. Thus, this unwrap should never fail as its only
            // called after submit has returned successfully.
            for slice in VarDataIterator::try_from(arg).unwrap() {
                self.push(slice.to_vec())
            }
            Ok(())
        }

        fn final_result(
            mut self,
            arg: RawReadOutput<Self::Unit>,
        ) -> Result<Self::Final, Self::Error> {
            self.intermediate_result(arg).map(|_| self)
        }

        fn cleared(&self) -> Option<Self> {
            Some(vec![])
        }
    }

    impl ReadCallback for Vec<String> {
        type Unit = u8;
        type Intermediate = ();
        type Final = Self;
        type Error = std::convert::Infallible;

        fn intermediate_result(
            &mut self,
            arg: RawReadOutput<Self::Unit>,
        ) -> Result<Self::Intermediate, Self::Error> {
            // TileDB Core currently ensures that all buffers are properly set
            // as required. Thus, this unwrap should never fail as its only
            // called after submit has returned successfully.
            for slice in VarDataIterator::try_from(arg).unwrap() {
                self.push(String::from_utf8_lossy(slice).to_string())
            }
            Ok(())
        }

        fn final_result(
            mut self,
            arg: RawReadOutput<Self::Unit>,
        ) -> Result<Self::Final, Self::Error> {
            self.intermediate_result(arg).map(|_| self)
        }

        fn cleared(&self) -> Option<Self> {
            Some(vec![])
        }
    }

    impl ReadCallback for (Vec<String>, Vec<u8>) {
        type Unit = u8;
        type Intermediate = ();
        type Final = Self;
        type Error = std::convert::Infallible;

        fn intermediate_result(
            &mut self,
            arg: RawReadOutput<Self::Unit>,
        ) -> Result<Self::Intermediate, Self::Error> {
            // Copy validity before VarDataIter consumes arg.
            self.1.extend_from_slice(
                &arg.input.validity.as_ref().unwrap().as_ref()[0..arg.ncells],
            );

            // TileDB Core currently ensures that all buffers are properly set
            // as required. Thus, this unwrap should never fail as its only
            // called after submit has returned successfully.
            for slice in VarDataIterator::try_from(arg).unwrap() {
                self.0.push(String::from_utf8_lossy(slice).to_string())
            }
            Ok(())
        }

        fn final_result(
            mut self,
            arg: RawReadOutput<Self::Unit>,
        ) -> Result<Self::Final, Self::Error> {
            self.intermediate_result(arg).map(|_| self)
        }

        fn cleared(&self) -> Option<Self> {
            Some((vec![], vec![]))
        }
    }
}

macro_rules! query_read_callback {
    ($query:ident, $callback:ident, $Builder:ident, $($U:ident),+) => {
        paste! {
            /// Query result handler which runs a callback on the results after each
            /// step of execution.
            pub struct $query<'data, T, Q>
            where
                T: $callback,
            {
                pub(crate) callback: Option<T>,
                pub(crate) base: Q,
                $(
                    pub(crate) [< arg_ $U:snake >]: RawReadHandle<'data, T::$U>
                ),+
            }

            impl<'data, T, Q> $query<'data, T, Q> where T: $callback {
                pub fn map_intermediate<F, O>(self, transform: F) -> $query<'data, MapIntermediate<T, F>, Q>
                where F: FnMut(T::Intermediate) -> O {
                    $query {
                        callback: self.callback.map(|callback| MapIntermediate {
                            callback,
                            transform
                        }),
                        base: self.base,
                        $(
                            [< arg_ $U:snake >]: self.[< arg_ $U:snake >]
                        ),+
                    }
                }

                pub fn map_final<F, O>(self, transform: F) -> $query<'data, MapFinal<T, F>, Q>
                where F: FnMut(T::Final) -> O {
                    $query {
                        callback: self.callback.map(|callback| MapFinal {
                            callback,
                            transform
                        }),
                        base: self.base,
                        $(
                            [< arg_ $U:snake >]: self.[< arg_ $U:snake >]
                        ),+
                    }
                }

                pub fn map<M>(self, transform: M) -> $query<'data, MapAdapter<M, T>, Q>
                where M: Map<T::Intermediate, T::Final>
                {
                    $query {
                        callback: self.callback.map(|callback| MapAdapter::new(transform, callback)),
                        base: self.base,
                        $(
                            [< arg_ $U:snake >]: self.[< arg_ $U:snake >]
                        ),+
                    }
                }
            }

            impl<'data, T, Q> ContextBound for $query<'data, T, Q>
            where
                T: $callback,
                Q: ContextBound,
            {
                fn context(&self) -> Context {
                    self.base.context()
                }
            }

            impl<'data, T, Q> Query for $query<'data, T, Q>
            where
                T: $callback,
                Q: Query,
            {
                fn base(&self) -> &QueryBase {
                    self.base.base()
                }

                fn finalize(self) -> TileDBResult<Array> {
                    self.base.finalize()
                }
            }
        }

        impl<'data, T, Q> $query<'data, T, Q> where T: $callback {
            fn realloc_managed_buffers(&mut self) {
                paste! {
                    $(
                        self.[< arg_ $U:snake >].realloc_if_managed();
                    )+
                }
            }
        }

        impl<'data, T, Q> ReadQuery for $query <'data, T, Q>
            where T: $callback,
                  Q: ReadQuery
        {
            type Intermediate = (T::Intermediate, Q::Intermediate);
            type Final = (T::Final, Q::Final);

            fn step(&mut self) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
                /*
                 * First we must attach all the buffers
                 */
                paste! {
                    $(
                        self.[< arg_ $U:snake >].attach_query(
                            &self.base().context(),
                            **self.base().cquery())?;
                    )+
                }

                let base_result = self.base.step()?;

                paste! {
                    $(
                        let [< ncells_ $U:snake >] = {
                            let ncells = self.[< arg_ $U:snake >].last_read_ncells();
                            if !base_result.is_final() {
                                if ncells == 0 {
                                    self.realloc_managed_buffers();
                                    return Ok(ReadStepOutput::NotEnoughSpace)
                                }
                            }
                            ncells
                        };

                        let [< l_ $U:snake >] = self.[< arg_ $U:snake >].location.borrow();
                        let [< input_ $U:snake >] = [< l_ $U:snake >].as_shared();
                            /*
                             * TODO: if it is the final result, enable moving out of
                             * this so that we can avoid a copy
                             */

                        let [< arg_ $U:snake >] = RawReadOutput {
                            ncells: [< ncells_ $U:snake >],
                            input: [< input_ $U:snake >],
                        };
                    )+
                }

                match base_result {
                    ReadStepOutput::NotEnoughSpace => unreachable!(),
                    ReadStepOutput::Intermediate(base_result) => {
                        let callback = match self.callback.as_mut() {
                            None => unimplemented!(),
                            Some(c) => c
                        };
                        let ir = paste! {
                            callback.intermediate_result(
                                $(
                                    [< arg_ $U:snake >],
                                )+
                            )
                                .map_err(|e| {
                                    let fields = paste! {
                                        vec![$(
                                            self.[< arg_ $U:snake >].field.name.clone()
                                        ),+]
                                    };
                                    crate::error::Error::QueryCallback(fields, anyhow!(e))
                                })?
                        };
                        Ok(ReadStepOutput::Intermediate((ir, base_result)))
                    },
                    ReadStepOutput::Final(base_result) => {
                        let callback_final = match self.callback.take() {
                            None => unimplemented!(),
                            Some(c) => {
                                self.callback = c.cleared();
                                c
                            }
                        };
                        let fr = paste! {
                            callback_final.final_result(
                                $(
                                    [< arg_ $U:snake >],
                                )+
                            )
                                .map_err(|e| {
                                    let fields = paste! {
                                        vec![$(
                                            self.[< arg_ $U:snake >].field.name.clone()
                                        ),+]
                                    };
                                    crate::error::Error::QueryCallback(fields, anyhow!(e))
                                })?
                        };
                        Ok(ReadStepOutput::Final((fr, base_result)))
                    }
                }
            }
        }

        paste! {
            pub struct $Builder<'data, T, B>
            where T: $callback,
            {
                pub(crate) callback: T,
                pub(crate) base: B,
                $(
                    pub(crate) [< arg_ $U:snake >]: RawReadHandle<'data, T::$U>
                ),+
            }

            impl<'data, T, B> $Builder<'data, T, B> where T: $callback {
                pub fn map_intermediate<F, O>(
                    self, transform: F
                ) -> $Builder<'data, MapIntermediate<T, F>, B>
                where F: FnMut(T::Intermediate) -> O {
                    $Builder {
                        callback: MapIntermediate {
                            callback: self.callback,
                            transform
                        },
                        base: self.base,
                        $(
                            [< arg_ $U:snake >]: self.[< arg_ $U:snake >]
                        ),+
                    }
                }

                pub fn map_final<F, O>(
                    self, transform: F
                ) -> $Builder<'data, MapFinal<T, F>, B>
                where F: FnMut(T::Final) -> O {
                    $Builder {
                        callback: MapFinal {
                            callback: self.callback,
                            transform
                        },
                        base: self.base,
                        $(
                            [< arg_ $U:snake >]: self.[< arg_ $U:snake >]
                        ),+
                    }
                }

                pub fn map<M>(self, transform: M) -> $Builder<'data, MapAdapter<M, T>, B>
                where M: Map<T::Intermediate, T::Final>
                {
                    $Builder {
                        callback: MapAdapter::new(transform, self.callback),
                        base: self.base,
                        $(
                            [< arg_ $U:snake >]: self.[< arg_ $U:snake >]
                        ),+
                    }
                }
            }

            impl<'data, T, B> QueryBuilder for $Builder <'data, T, B>
            where T: $callback,
                  B: QueryBuilder,
            {
                type Query = $query<'data, T, B::Query>;

                fn base(&self) -> &BuilderBase {
                    self.base.base()
                }

                fn build(self) -> Self::Query {
                    $query {
                        callback: Some(self.callback),
                        base: self.base.build(),
                        $(
                            [< arg_ $U:snake >]: self.[< arg_ $U:snake >]
                        ),+
                    }
                }
            }

            impl<'data, T, B> ReadQueryBuilder<'data> for $Builder<'data, T, B>
            where
                T: $callback,
                B: ReadQueryBuilder<'data>,
            {
            }
        }
    }
}

query_read_callback!(
    CallbackReadQuery,
    ReadCallback,
    CallbackReadBuilder,
    Unit
);

query_read_callback!(
    Callback2ArgReadQuery,
    ReadCallback2Arg,
    Callback2ArgReadBuilder,
    Unit1,
    Unit2
);

query_read_callback!(
    Callback3ArgReadQuery,
    ReadCallback3Arg,
    Callback3ArgReadBuilder,
    Unit1,
    Unit2,
    Unit3
);

query_read_callback!(
    Callback4ArgReadQuery,
    ReadCallback4Arg,
    Callback4ArgReadBuilder,
    Unit1,
    Unit2,
    Unit3,
    Unit4
);

pub struct CallbackVarArgReadQuery<'data, T, Q> {
    pub(crate) callback: Option<T>,
    pub(crate) base: VarRawReadQuery<'data, Q>,
}

impl<'data, T, Q> CallbackVarArgReadQuery<'data, T, Q>
where
    T: ReadCallbackVarArg,
{
    pub fn map_intermediate<F, O>(
        self,
        transform: F,
    ) -> CallbackVarArgReadQuery<'data, MapIntermediate<T, F>, Q>
    where
        F: FnMut(T::Intermediate) -> O,
    {
        CallbackVarArgReadQuery {
            callback: self.callback.map(|callback| MapIntermediate {
                callback,
                transform,
            }),
            base: self.base,
        }
    }

    pub fn map_final<F, O>(
        self,
        transform: F,
    ) -> CallbackVarArgReadQuery<'data, MapFinal<T, F>, Q>
    where
        F: FnMut(T::Final) -> O,
    {
        CallbackVarArgReadQuery {
            callback: self.callback.map(|callback| MapFinal {
                callback,
                transform,
            }),
            base: self.base,
        }
    }

    pub fn map<M>(
        self,
        transform: M,
    ) -> CallbackVarArgReadQuery<'data, MapAdapter<M, T>, Q>
    where
        M: Map<T::Intermediate, T::Final>,
    {
        CallbackVarArgReadQuery {
            callback: self.callback.map(|callback| MapAdapter {
                callback,
                transform,
            }),
            base: self.base,
        }
    }
}

impl<'data, T, Q> ContextBound for CallbackVarArgReadQuery<'data, T, Q>
where
    VarRawReadQuery<'data, Q>: ContextBound,
{
    fn context(&self) -> Context {
        self.base.context()
    }
}

impl<'data, T, Q> Query for CallbackVarArgReadQuery<'data, T, Q>
where
    VarRawReadQuery<'data, Q>: Query,
{
    fn base(&self) -> &QueryBase {
        self.base.base()
    }

    fn finalize(self) -> TileDBResult<Array> {
        self.base.finalize()
    }
}

impl<T, Q> ReadQuery for CallbackVarArgReadQuery<'_, T, Q>
where
    T: ReadCallbackVarArg,
    Q: ReadQuery,
{
    type Intermediate = (T::Intermediate, Q::Intermediate);
    type Final = (T::Final, Q::Final);

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        let base_result = self.base.step()?;

        match base_result {
            ReadStepOutput::NotEnoughSpace => {
                Ok(ReadStepOutput::NotEnoughSpace)
            }
            ReadStepOutput::Intermediate((sizes, base_result)) => {
                let callback = match self.callback.as_mut() {
                    None => unimplemented!(),
                    Some(c) => c,
                };

                let buffers = self
                    .base
                    .raw_read_output
                    .iter()
                    .map(|r| (r.field(), r.borrow_mut()))
                    .collect::<Vec<(&FieldMetadata, RefTypedQueryBuffersMut)>>(
                    );
                let args = sizes
                    .iter()
                    .zip(buffers.iter())
                    .map(|(ncells, (field, buffers))| TypedRawReadOutput {
                        datatype: field.datatype,
                        ncells: *ncells,
                        buffers: buffers.as_shared(),
                    })
                    .collect::<Vec<TypedRawReadOutput>>();

                let ir = callback.intermediate_result(args).map_err(|e| {
                    let fields = self
                        .base
                        .raw_read_output
                        .iter()
                        .map(|rh| rh.field().name.clone())
                        .collect::<Vec<String>>();
                    crate::error::Error::QueryCallback(fields, anyhow!(e))
                })?;
                Ok(ReadStepOutput::Intermediate((ir, base_result)))
            }
            ReadStepOutput::Final((sizes, base_result)) => {
                let callback_final = match self.callback.take() {
                    None => unimplemented!(),
                    Some(c) => {
                        self.callback = c.cleared();
                        c
                    }
                };

                let buffers = self
                    .base
                    .raw_read_output
                    .iter()
                    .map(|r| (r.field(), r.borrow_mut()))
                    .collect::<Vec<(&FieldMetadata, RefTypedQueryBuffersMut)>>(
                    );
                let args = sizes
                    .iter()
                    .zip(buffers.iter())
                    .map(|(ncells, (field, buffers))| TypedRawReadOutput {
                        datatype: field.datatype,
                        ncells: *ncells,
                        buffers: buffers.as_shared(),
                    })
                    .collect::<Vec<TypedRawReadOutput>>();

                let ir = callback_final.final_result(args).map_err(|e| {
                    let fields = self
                        .base
                        .raw_read_output
                        .iter()
                        .map(|rh| rh.field().name.clone())
                        .collect::<Vec<String>>();
                    crate::error::Error::QueryCallback(fields, anyhow!(e))
                })?;
                Ok(ReadStepOutput::Final((ir, base_result)))
            }
        }
    }
}

pub struct CallbackVarArgReadBuilder<'data, T, B> {
    pub(crate) callback: T,
    pub(crate) base: VarRawReadBuilder<'data, B>,
}

impl<'data, T, B> CallbackVarArgReadBuilder<'data, T, B>
where
    T: ReadCallbackVarArg,
{
    pub fn map_intermediate<F, O>(
        self,
        transform: F,
    ) -> CallbackVarArgReadBuilder<'data, MapIntermediate<T, F>, B>
    where
        F: FnMut(T::Intermediate) -> O,
    {
        CallbackVarArgReadBuilder {
            callback: MapIntermediate {
                callback: self.callback,
                transform,
            },
            base: self.base,
        }
    }

    pub fn map_final<F, O>(
        self,
        transform: F,
    ) -> CallbackVarArgReadBuilder<'data, MapFinal<T, F>, B>
    where
        F: FnMut(T::Final) -> O,
    {
        CallbackVarArgReadBuilder {
            callback: MapFinal {
                callback: self.callback,
                transform,
            },
            base: self.base,
        }
    }

    pub fn map<M>(
        self,
        transform: M,
    ) -> CallbackVarArgReadBuilder<'data, MapAdapter<M, T>, B>
    where
        M: Map<T::Intermediate, T::Final>,
    {
        CallbackVarArgReadBuilder {
            callback: MapAdapter {
                callback: self.callback,
                transform,
            },
            base: self.base,
        }
    }
}

impl<'data, T, B> ContextBound for CallbackVarArgReadBuilder<'data, T, B>
where
    VarRawReadBuilder<'data, B>: ContextBound,
{
    fn context(&self) -> Context {
        self.base.context()
    }
}

impl<'data, T, B> QueryBuilder for CallbackVarArgReadBuilder<'data, T, B>
where
    B: QueryBuilder,
{
    type Query = CallbackVarArgReadQuery<'data, T, B::Query>;

    fn base(&self) -> &BuilderBase {
        self.base.base()
    }

    fn build(self) -> Self::Query {
        CallbackVarArgReadQuery {
            callback: Some(self.callback),
            base: self.base.build(),
        }
    }
}

impl<'data, T, B> ReadQueryBuilder<'data>
    for CallbackVarArgReadBuilder<'data, T, B>
where
    B: QueryBuilder,
{
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

    use crate::query::buffer::{Buffer, CellStructure, QueryBuffers};
    use crate::query::read::output::{
        NonVarSized, ScratchCellStructure, VarSized,
    };

    const MIN_RECORDS: usize = 0;
    const MAX_RECORDS: usize = 1024;

    const MIN_BYTE_CAPACITY: usize = 0;
    const MAX_BYTE_CAPACITY: usize = 1024 * 1024;

    fn do_read_result_repr<C>(dst_unit_capacity: usize, unitsrc: Vec<C>)
    where
        C: CellValue,
    {
        let alloc = NonVarSized {
            capacity: dst_unit_capacity,
            cell_val_num: NonZeroU32::new(1).unwrap(),
        };

        let mut scratch_space = alloc.alloc();

        let mut unitdst = <Vec<C> as Default>::default();

        while unitdst.len() < unitsrc.len() {
            let ncells = std::cmp::min(
                scratch_space.0.len(),
                unitsrc.len() - unitdst.len(),
            );
            if ncells == 0 {
                scratch_space = alloc.realloc(scratch_space);
                continue;
            }

            unsafe {
                std::ptr::copy_nonoverlapping::<C>(
                    unitsrc[unitdst.len()..unitsrc.len()].as_ptr(),
                    scratch_space.0.as_mut_ptr(),
                    ncells,
                )
            };

            let input_data = QueryBuffers {
                data: Buffer::Borrowed(&scratch_space.0),
                cell_structure: CellStructure::Fixed(
                    NonZeroU32::new(1).unwrap(),
                ),
                validity: None,
            };

            let arg = RawReadOutput {
                ncells,
                input: input_data,
            };

            let prev_len = unitdst.len();

            <Vec<C> as ReadCallback>::intermediate_result(&mut unitdst, arg)
                .expect("Error aggregating data into Vec");

            assert_eq!(ncells, unitdst.len() - prev_len);
            assert_eq!(unitsrc[0..unitdst.len()], unitdst);
        }

        assert_eq!(unitsrc, unitdst);
    }

    proptest! {
        #[test]
        fn read_result_u64(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<u64>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_read_result_repr::<u64>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn read_result_u32(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<u32>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_read_result_repr::<u32>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn read_result_u16(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<u16>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_read_result_repr::<u16>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn read_result_u8(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<u8>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_read_result_repr::<u8>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn read_result_f64(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<f64>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_read_result_repr::<f64>(dst_unit_capacity, unitsrc)
        }

        #[test]
        fn read_result_f32(dst_unit_capacity in MIN_RECORDS..=MAX_RECORDS, unitsrc in vec(any::<f32>(), MIN_RECORDS..=MAX_RECORDS)) {
            do_read_result_repr::<f32>(dst_unit_capacity, unitsrc)
        }
    }

    fn do_read_result_strings(
        record_capacity: usize,
        byte_capacity: usize,
        stringsrc: Vec<String>,
    ) {
        let alloc = VarSized {
            byte_capacity,
            offset_capacity: record_capacity,
        };

        let mut scratch_space = alloc.alloc();

        let mut stringdst: Vec<String> = vec![];

        while stringdst.len() < stringsrc.len() {
            /* copy from stringsrc to scratch data */
            let ncells = {
                /* write the offsets first */
                let (ncells, nbytes) = {
                    let scratch_offsets =
                        scratch_space.1.offsets_mut().unwrap();
                    let mut off = 0;
                    let mut src =
                        stringsrc[stringdst.len()..stringsrc.len()].iter();

                    if scratch_offsets.len() <= 1 {
                        // if there are any offsets there are at least two
                        (0, 0)
                    } else {
                        scratch_offsets[0] = 0;

                        let mut i = 1;
                        let (noffsets, nbytes) = loop {
                            if i >= scratch_offsets.len() {
                                break (i, off);
                            }
                            if let Some(src) = src.next() {
                                if off + src.len() <= scratch_space.0.len() {
                                    off += src.len();
                                    scratch_offsets[i] = off as u64;
                                    i += 1;
                                } else {
                                    break (i, off);
                                }
                            } else {
                                break (i, off);
                            }
                        };
                        (noffsets - 1, nbytes)
                    }
                };

                if ncells == 0 {
                    assert_eq!(0, nbytes);
                    scratch_space = alloc.realloc(scratch_space);
                    continue;
                }

                let scratch_offsets = scratch_space.1.offsets_ref().unwrap();

                /* then transfer contents */
                for i in 0..ncells {
                    let s = &stringsrc[stringdst.len() + i];
                    let start = scratch_offsets[i] as usize;
                    let end = scratch_offsets[i + 1] as usize;
                    scratch_space.0[start..end].copy_from_slice(s.as_bytes())
                }

                ncells
            };

            /* then copy from scratch data to stringdst */
            let prev_len = stringdst.len();
            let input = QueryBuffers {
                data: Buffer::Borrowed(&scratch_space.0),
                cell_structure: match scratch_space.1 {
                    ScratchCellStructure::Fixed(nz) => CellStructure::Fixed(nz),
                    ScratchCellStructure::Var(ref offsets) => {
                        CellStructure::Var(Buffer::Borrowed(offsets.as_ref()))
                    }
                },
                validity: scratch_space.2.as_ref().map(|v| Buffer::Borrowed(v)),
            };
            let arg = RawReadOutput { ncells, input };
            stringdst
                .intermediate_result(arg)
                .expect("Error aggregating Vec<String>");

            assert_eq!(ncells, stringdst.len() - prev_len);
            assert_eq!(stringsrc[0..stringdst.len()], stringdst);
        }
    }

    proptest! {
        #[test]
        fn read_result_strings(
            record_capacity in MIN_RECORDS..=MAX_RECORDS,
            byte_capacity in MIN_BYTE_CAPACITY..=MAX_BYTE_CAPACITY,
            stringsrc in crate::query::buffer::tests::prop_string_vec(
                (MIN_RECORDS..=MAX_RECORDS).into()
            )
        )
        {
            do_read_result_strings(
                record_capacity,
                byte_capacity,
                stringsrc
            )
        }
    }
}
