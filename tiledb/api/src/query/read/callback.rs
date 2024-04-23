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
                type $U: CAPISameRepr;
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
        args: &[TypedRawReadOutput],
    ) -> Result<Self::Intermediate, Self::Error>;

    fn final_result(
        self,
        args: &[TypedRawReadOutput],
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
    <A as FromQueryOutput>::Unit: CAPISameRepr,
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
                <$A as FromQueryOutput>::Unit: CAPISameRepr
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

mod impls {
    use super::*;
    use crate::query::read::output::VarDataIterator;

    impl<C> ReadCallback for Vec<C>
    where
        C: CAPISameRepr,
    {
        type Unit = C;
        type Intermediate = ();
        type Final = Self;
        type Error = std::convert::Infallible;

        fn intermediate_result(
            &mut self,
            arg: RawReadOutput<Self::Unit>,
        ) -> Result<Self::Intermediate, Self::Error> {
            self.extend_from_slice(&arg.input.data.as_ref()[0..arg.nvalues]);
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
        C: CAPISameRepr,
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
                .extend_from_slice(&arg.input.data.as_ref()[0..arg.nvalues]);
            // TileDB Core currently ensures that all buffers are properly set
            // as required. Thus, this unwrap should never fail as its only
            // called after submit has returned successfully.
            self.1.extend_from_slice(
                &arg.input.validity.as_ref().unwrap().as_ref()[0..arg.nvalues],
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
        C: CAPISameRepr,
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
                &arg.input.validity.as_ref().unwrap().as_ref()[0..arg.nvalues],
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
            #[derive(ContextBound, Query)]
            pub struct $query<'data, T, Q>
            where
                T: $callback,
            {
                pub(crate) callback: Option<T>,
                #[base(ContextBound, Query)]
                pub(crate) base: Q,
                $(
                    pub(crate) [< arg_ $U:snake >]: RawReadHandle<'data, T::$U>
                ),+
            }
        }

        impl<'ctx, 'data, T, Q> ReadQuery<'ctx> for $query <'data, T, Q>
            where T: $callback,
                  Q: ReadQuery<'ctx>
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
                            self.base().context(),
                            **self.base().cquery())?;
                    )+
                }

                let base_result = self.base.step()?;

                paste! {
                    $(
                        let ([< nvalues_ $U:snake >], [< nbytes_ $U:snake >]) = {
                            let (nvalues, nbytes) = self.[< arg_ $U:snake >].last_read_size();
                            if !base_result.is_final() {
                                if nvalues == 0 && nbytes == 0 {
                                    return Ok(ReadStepOutput::NotEnoughSpace)
                                } else if nvalues == 0 {
                                    return Err(Error::Internal(format!(
                                                "Invalid read: returned {} offsets but {} bytes",
                                                nvalues, nbytes
                                    )));
                                }
                            }
                            (nvalues, nbytes)
                        };

                        let [< l_ $U:snake >] = self.[< arg_ $U:snake >].location.borrow();
                        let [< input_ $U:snake >] = [< l_ $U:snake >].as_shared();

                        let [< arg_ $U:snake >] = RawReadOutput {
                            nvalues: [< nvalues_ $U:snake >],
                            nbytes: [< nbytes_ $U:snake >],
                            input: &[< input_ $U:snake >]
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
                                            self.[< arg_ $U:snake >].field.clone()
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
                                            self.[< arg_ $U:snake >].field.clone()
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
            #[derive(ContextBound)]
            pub struct $Builder<'data, T, B>
            where T: $callback,
            {
                pub(crate) callback: T,
                #[base(ContextBound)]
                pub(crate) base: B,
                $(
                    pub(crate) [< arg_ $U:snake >]: RawReadHandle<'data, T::$U>
                ),+
            }

            impl<'ctx, 'data, T, B> QueryBuilder<'ctx> for $Builder <'data, T, B>
            where T: $callback,
                  B: QueryBuilder<'ctx>,
            {
                type Query = $query<'data, T, B::Query>;

                fn base(&self) -> &BuilderBase<'ctx> {
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

            impl<'ctx, 'data, T, B> ReadQueryBuilder<'ctx, 'data> for $Builder<'data, T, B>
            where
                T: $callback,
                B: ReadQueryBuilder<'ctx, 'data>,
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

#[derive(ContextBound, Query)]
pub struct CallbackVarArgReadQuery<'data, T, Q> {
    pub(crate) callback: Option<T>,
    #[base(ContextBound, Query)]
    pub(crate) base: VarRawReadQuery<'data, Q>,
}

impl<'ctx, 'data, T, Q> ReadQuery<'ctx> for CallbackVarArgReadQuery<'data, T, Q>
where
    T: ReadCallbackVarArg,
    Q: ReadQuery<'ctx>,
{
    type Intermediate = (T::Intermediate, Q::Intermediate);
    type Final = (T::Final, Q::Final);

    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        let base_result = self.base.step()?;

        match base_result {
            ReadStepOutput::NotEnoughSpace => unreachable!(),
            ReadStepOutput::Intermediate((sizes, base_result)) => {
                let callback = match self.callback.as_mut() {
                    None => unimplemented!(),
                    Some(c) => c,
                };

                let buffers = self
                    .base
                    .raw_read_output
                    .iter()
                    .map(|r| r.borrow())
                    .collect::<Vec<RefTypedQueryBuffersMut>>();
                let args = sizes
                    .iter()
                    .zip(buffers.iter())
                    .map(|(&(nvalues, nbytes), buffers)| TypedRawReadOutput {
                        nvalues,
                        nbytes,
                        buffers: buffers.as_shared(),
                    })
                    .collect::<Vec<TypedRawReadOutput>>();

                let ir = callback.intermediate_result(&args).map_err(|e| {
                    let fields = self
                        .base
                        .raw_read_output
                        .iter()
                        .map(|rh| rh.field().clone())
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
                    .map(|r| r.borrow())
                    .collect::<Vec<RefTypedQueryBuffersMut>>();
                let args = sizes
                    .iter()
                    .zip(buffers.iter())
                    .map(|(&(nvalues, nbytes), buffers)| TypedRawReadOutput {
                        nvalues,
                        nbytes,
                        buffers: buffers.as_shared(),
                    })
                    .collect::<Vec<TypedRawReadOutput>>();

                let ir = callback_final.final_result(&args).map_err(|e| {
                    let fields = self
                        .base
                        .raw_read_output
                        .iter()
                        .map(|rh| rh.field().clone())
                        .collect::<Vec<String>>();
                    crate::error::Error::QueryCallback(fields, anyhow!(e))
                })?;
                Ok(ReadStepOutput::Final((ir, base_result)))
            }
        }
    }
}

#[derive(ContextBound)]
pub struct CallbackVarArgReadBuilder<'data, T, B> {
    pub(crate) callback: T,
    #[base(ContextBound)]
    pub(crate) base: VarRawReadBuilder<'data, B>,
}

impl<'ctx, 'data, T, B> QueryBuilder<'ctx>
    for CallbackVarArgReadBuilder<'data, T, B>
where
    B: QueryBuilder<'ctx>,
{
    type Query = CallbackVarArgReadQuery<'data, T, B::Query>;

    fn base(&self) -> &BuilderBase<'ctx> {
        self.base.base()
    }

    fn build(self) -> Self::Query {
        CallbackVarArgReadQuery {
            callback: Some(self.callback),
            base: self.base.build(),
        }
    }
}

impl<'ctx, 'data, T, B> ReadQueryBuilder<'ctx, 'data>
    for CallbackVarArgReadBuilder<'data, T, B>
where
    B: QueryBuilder<'ctx>,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

    use crate::query::buffer::{Buffer, QueryBuffers};
    use crate::query::read::output::{NonVarSized, VarSized};

    const MIN_RECORDS: usize = 0;
    const MAX_RECORDS: usize = 1024;

    const MIN_BYTE_CAPACITY: usize = 0;
    const MAX_BYTE_CAPACITY: usize = 1024 * 1024;

    fn do_read_result_repr<C>(dst_unit_capacity: usize, unitsrc: Vec<C>)
    where
        C: CAPISameRepr + std::fmt::Debug + PartialEq,
    {
        let alloc = NonVarSized {
            capacity: dst_unit_capacity,
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
                cell_offsets: None,
                validity: None,
            };

            let arg = RawReadOutput {
                nvalues: ncells,
                nbytes: ncells * std::mem::size_of::<u64>(),
                input: &input_data,
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
            let (nvalues, nbytes) = {
                /* write the offsets first */
                let (nvalues, nbytes) = {
                    let scratch_offsets = scratch_space.1.as_mut().unwrap();
                    let mut i = 0;
                    let mut off = 0;
                    let mut src =
                        stringsrc[stringdst.len()..stringsrc.len()].iter();
                    loop {
                        if i >= scratch_offsets.len() {
                            break (i, off);
                        }
                        if let Some(src) = src.next() {
                            if off + src.len() <= scratch_space.0.len() {
                                scratch_offsets[i] = off as u64;
                                off += src.len();
                            } else {
                                break (i, off);
                            }
                        } else {
                            break (i, off);
                        }
                        i += 1;
                    }
                };

                if nvalues == 0 {
                    assert_eq!(0, nbytes);
                    scratch_space = alloc.realloc(scratch_space);
                    continue;
                }

                let scratch_offsets = scratch_space.1.as_ref().unwrap();

                /* then transfer contents */
                for i in 0..nvalues {
                    let s = &stringsrc[stringdst.len() + i];
                    let start = scratch_offsets[i] as usize;
                    let end = if i + 1 < nvalues {
                        scratch_offsets[i + 1] as usize
                    } else {
                        nbytes
                    };
                    scratch_space.0[start..end].copy_from_slice(s.as_bytes())
                }

                (nvalues, nbytes)
            };

            /* then copy from scratch data to stringdst */
            let prev_len = stringdst.len();
            let input = QueryBuffers {
                data: Buffer::Borrowed(&scratch_space.0),
                cell_offsets: scratch_space
                    .1
                    .as_ref()
                    .map(|c| Buffer::Borrowed(c)),
                validity: scratch_space.2.as_ref().map(|v| Buffer::Borrowed(v)),
            };
            let arg = RawReadOutput {
                nvalues,
                nbytes,
                input: &input,
            };
            stringdst
                .intermediate_result(arg)
                .expect("Error aggregating Vec<String>");

            assert_eq!(nvalues, stringdst.len() - prev_len);
            assert_eq!(stringsrc[0..stringdst.len()], stringdst);
        }
    }

    proptest! {
        #[test]
        fn read_result_strings(
            record_capacity in MIN_RECORDS..=MAX_RECORDS,
            byte_capacity in MIN_BYTE_CAPACITY..=MAX_BYTE_CAPACITY,
            stringsrc in crate::query::buffer::strategy::prop_string_vec(
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
