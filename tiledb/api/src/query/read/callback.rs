use super::*;

use anyhow::anyhow;
use itertools::izip;
use paste::paste;

use crate::query::read::output::FromQueryOutput;
use crate::query::read::splitter::{ReadSplitterBuilder, ReadSplitterQuery};
use crate::query::write::input::{Buffer, InputData};

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
                        [< records_ $U:snake >]: usize,
                        [< bytes_ $U:snake >]: usize,
                        [< input_ $U:snake >]: InputData<'_, Self::$U>,
                    )+
                ) -> Result<Self::Intermediate, Self::Error>;

                fn final_result(
                    self,
                    $(
                        [< records_ $U:snake >]: usize,
                        [< bytes_ $U:snake >]: usize,
                        [< input_ $U:snake >]: InputData<'_, Self::$U>,
                    )+
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

#[derive(Clone)]
pub struct FnMutAdapter<A, F> {
    arg: std::marker::PhantomData<A>,
    func: F,
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
        records: usize,
        bytes: usize,
        input: InputData<Self::Unit>,
    ) -> Result<Self::Intermediate, Self::Error> {
        let iter = <A as FromQueryOutput>::Iterator::try_from((
            records, bytes, &input,
        ))?;

        for record in iter {
            (self.func)(record)
        }

        Ok(())
    }

    fn final_result(
        mut self,
        records: usize,
        bytes: usize,
        input: InputData<Self::Unit>,
    ) -> Result<Self::Intermediate, Self::Error> {
        let iter = <A as FromQueryOutput>::Iterator::try_from((
            records, bytes, &input,
        ))?;

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
                        [< nrecords_ $A:snake >]: usize,
                        [< nbytes_ $A:snake >]: usize,
                        [< input_ $A:snake >]: InputData<Self::$U>,
                    )+
                ) -> Result<Self::Intermediate, Self::Error>
                {
                    $(
                        let [< iter_ $A:snake >] = <$A as FromQueryOutput>::Iterator::try_from((
                            [< nrecords_ $A:snake >],
                            [< nbytes_ $A:snake >],
                            &[< input_ $A:snake >],
                        ))?;
                    )+

                    for ($([< r_ $A:snake >]),+) in izip!($([< iter_ $A:snake >]),+) {
                        (self.func)($([< r_ $A:snake >]),+)
                    }

                    Ok(())
                }

                fn final_result(
                    mut self,
                    $(
                        [< nrecords_ $A:snake >]: usize,
                        [< nbytes_ $A:snake >]: usize,
                        [< input_ $A:snake >]: InputData<Self::$U>,
                    )+
                ) -> Result<Self::Final, Self::Error>
                {
                    $(
                        let [< iter_ $A:snake >] = <$A as FromQueryOutput>::Iterator::try_from((
                            [< nrecords_ $A:snake >],
                            [< nbytes_ $A:snake >],
                            &[< input_ $A:snake >],
                        ))?;
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

/// Query result handler which runs a callback on the results after each
/// step of execution.
#[derive(ContextBound, QueryCAPIInterface)]
pub struct CallbackReadQuery<'data, T, Q>
where
    T: ReadCallback,
{
    pub(crate) callback: Option<T>,
    #[base(ContextBound, QueryCAPIInterface)]
    pub(crate) base: RawReadQuery<'data, T::Unit, Q>,
}

impl<'ctx, 'data, T, Q> ReadQuery for CallbackReadQuery<'data, T, Q>
where
    T: ReadCallback,
    Q: ReadQuery + ContextBound<'ctx> + QueryCAPIInterface,
{
    type Intermediate = (T::Intermediate, Q::Intermediate);
    type Final = (T::Final, Q::Final);

    /// Run the query until it fills the scratch space.
    /// Invokes the callback on all data in the scratch space when the query returns.
    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        let base_result = self.base.step()?;

        let location = self.base.raw_read_output.location.borrow();

        /*
         * TODO:
         * If the buffer is managed and this is the final result then
         * there's a chance the callback will benefit from owning the buffer
         * rather than borrowing it
         */
        let input_data = InputData {
            data: Buffer::Borrowed(&location.data),
            cell_offsets: location
                .cell_offsets
                .as_ref()
                .map(|c| Buffer::Borrowed(c)),
        };

        Ok(match base_result {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate((nrecords, nbytes, base_result)) => {
                let callback = match self.callback.as_mut() {
                    None => unimplemented!(),
                    Some(c) => c,
                };
                let ir = callback
                    .intermediate_result(nrecords, nbytes, input_data)
                    .map_err(|e| {
                        crate::error::Error::QueryCallback(
                            vec![self.base.field.clone()],
                            anyhow!(e),
                        )
                    })?;
                ReadStepOutput::Intermediate((ir, base_result))
            }
            ReadStepOutput::Final((nrecords, nbytes, base_result)) => {
                let callback_final = match self.callback.take() {
                    None => unimplemented!(),
                    Some(c) => {
                        self.callback = c.cleared();
                        c
                    }
                };
                let fr = callback_final
                    .final_result(nrecords, nbytes, input_data)
                    .map_err(|e| {
                        crate::error::Error::QueryCallback(
                            vec![self.base.field.clone()],
                            anyhow!(e),
                        )
                    })?;
                ReadStepOutput::Final((fr, base_result))
            }
        })
    }
}

#[derive(ContextBound, QueryCAPIInterface)]
pub struct CallbackReadBuilder<'data, T, B>
where
    T: ReadCallback,
{
    pub(crate) callback: T,
    #[base(ContextBound, QueryCAPIInterface)]
    pub(crate) base: RawReadBuilder<'data, <T as ReadCallback>::Unit, B>,
}

impl<'ctx, 'data, T, B> QueryBuilder<'ctx> for CallbackReadBuilder<'data, T, B>
where
    T: ReadCallback,
    B: QueryBuilder<'ctx>,
{
    type Query = CallbackReadQuery<'data, T, B::Query>;

    fn build(self) -> Self::Query {
        CallbackReadQuery {
            callback: Some(self.callback),
            base: self.base.build(),
        }
    }
}

impl<'ctx, 'data, T, B> ReadQueryBuilder<'ctx>
    for CallbackReadBuilder<'data, T, B>
where
    T: ReadCallback,
    B: ReadQueryBuilder<'ctx>,
{
}

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
            records: usize,
            _bytes: usize,
            input: InputData<'_, C>,
        ) -> Result<Self::Intermediate, Self::Error> {
            if let Buffer::Owned(data) = input.data {
                if self.is_empty() {
                    *self = data.into_vec();
                    self.truncate(records)
                } else {
                    self.extend_from_slice(&data[0..records])
                }
            } else {
                self.extend_from_slice(&input.data.as_ref()[0..records])
            };
            Ok(())
        }

        fn final_result(
            mut self,
            records: usize,
            bytes: usize,
            input: InputData<'_, C>,
        ) -> Result<Self::Final, Self::Error> {
            if self.is_empty() {
                if let Buffer::Owned(data) = input.data {
                    let mut v = data.into_vec();
                    v.truncate(records);
                    return Ok(v);
                }
            }
            self.intermediate_result(records, bytes, input)
                .map(|_| self)
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
            records: usize,
            bytes: usize,
            input: InputData<'_, C>,
        ) -> Result<Self::Intermediate, Self::Error> {
            for slice in VarDataIterator::new(records, bytes, &input).unwrap() {
                self.push(slice.to_vec())
            }
            Ok(())
        }

        fn final_result(
            mut self,
            records: usize,
            bytes: usize,
            input: InputData<'_, C>,
        ) -> Result<Self::Final, Self::Error> {
            self.intermediate_result(records, bytes, input)
                .map(|_| self)
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
            records: usize,
            bytes: usize,
            input: InputData<'_, u8>,
        ) -> Result<Self::Intermediate, Self::Error> {
            for slice in VarDataIterator::new(records, bytes, &input).unwrap() {
                self.push(String::from_utf8_lossy(slice).to_string())
            }
            Ok(())
        }

        fn final_result(
            mut self,
            records: usize,
            bytes: usize,
            input: InputData<'_, u8>,
        ) -> Result<Self::Final, Self::Error> {
            self.intermediate_result(records, bytes, input)
                .map(|_| self)
        }

        fn cleared(&self) -> Option<Self> {
            Some(vec![])
        }
    }
}

macro_rules! query_read_callback {
    ($query:ident, $callback:ident, $Builder:ident, $($U:ident),+) => {
        paste! {
            /// Query result handler which runs a callback on the results after each
            /// step of execution.
            #[derive(ContextBound, QueryCAPIInterface)]
            pub struct $query<'ctx, 'data, T, Q>
            where
                T: $callback,
                Q: ReadQuery
            {
                pub(crate) callback: Option<T>,
                #[base(ContextBound, QueryCAPIInterface)]
                pub(crate) query_base: Q,
                pub(crate) split_base: ReadSplitterQuery<'ctx>,
                $(
                    pub(crate) [< arg_ $U:snake >]: RawReadQuery<'data, T::$U, ReadSplitterQuery<'ctx>>
                ),+
            }
        }

        impl<'ctx, 'data, T, Q> ReadQuery for $query <'ctx, 'data, T, Q>
            where T: $callback,
                  Q: ReadQuery + ContextBound<'ctx> + QueryCAPIInterface
        {
            type Intermediate = (T::Intermediate, Q::Intermediate);
            type Final = (T::Final, Q::Final);

            fn step(&mut self) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
                let base_result = self.query_base.step()?;

                {
                    let mut previous_step = self.split_base.previous_step.borrow_mut();
                    *previous_step = Some(base_result.as_ref().map_i(|_| ()).map_f(|_| ()));
                }

                /*
                 * Assumption: each of the arguments and split will produce
                 * the same ReadStepOutput enum variant
                 */

                paste! {
                    $(
                        let ([< nrecords_ $U:snake >], [< nbytes_ $U:snake >]) =
                            match self.[< arg_ $U:snake >].step()?.unwrap()
                        {
                            None => return Ok(ReadStepOutput::NotEnoughSpace),
                            Some((nrecords, nbytes, ())) => (nrecords, nbytes)
                        };

                        let [< l_ $U:snake >] = self.[< arg_ $U:snake >].raw_read_output.location.borrow();
                        let [< input_ $U:snake >] = InputData {
                            data: Buffer::Borrowed(&[< l_ $U:snake >].data),
                            cell_offsets: [< l_ $U:snake >].cell_offsets.as_ref().map(|c| Buffer::Borrowed(c))
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
                                    [< nrecords_ $U:snake >],
                                    [< nbytes_ $U:snake >],
                                    [< input_ $U:snake >],
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
                                    [< nrecords_ $U:snake >],
                                    [< nbytes_ $U:snake >],
                                    [< input_ $U:snake >],
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
            #[derive(ContextBound, QueryCAPIInterface)]
            pub struct $Builder<'ctx, 'data, T, B>
            where T: $callback,
                  B: QueryBuilder<'ctx>,
                  <B as QueryBuilder<'ctx>>::Query: ReadQuery + ContextBound<'ctx> + QueryCAPIInterface + 'static
            {
                pub(crate) callback: T,
                #[base(ContextBound, QueryCAPIInterface)]
                pub(crate) query_base: B,
                pub(crate) split_base: ReadSplitterBuilder<'ctx>,
                $(
                    pub(crate) [< arg_ $U:snake >]: RawReadBuilder<'data, T::$U, ReadSplitterBuilder<'ctx>>
                ),+
            }

            impl<'ctx, 'data, T, B> QueryBuilder<'ctx> for $Builder <'ctx, 'data, T, B>
            where
                T: $callback,
                  B: QueryBuilder<'ctx>,
                  <B as QueryBuilder<'ctx>>::Query: ReadQuery + ContextBound<'ctx> + QueryCAPIInterface + 'static
            {
                type Query = $query<'ctx, 'data, T, B::Query>;

                fn build(self) -> Self::Query {
                    $query {
                        callback: Some(self.callback),
                        query_base: self.query_base.build(),
                        split_base: self.split_base.build(),
                        $(
                            [< arg_ $U:snake >]: self.[< arg_ $U:snake >].build()
                        ),+
                    }
                }
            }
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

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

            let input_data = InputData {
                data: Buffer::Borrowed(&scratch_space.0),
                cell_offsets: None,
            };

            let prev_len = unitdst.len();

            <Vec<C> as ReadCallback>::intermediate_result(
                &mut unitdst,
                ncells,
                ncells * std::mem::size_of::<u64>(),
                input_data,
            )
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
            let (nrecords, nbytes) = {
                /* write the offsets first */
                let (nrecords, nbytes) = {
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

                if nrecords == 0 {
                    assert_eq!(0, nbytes);
                    scratch_space = alloc.realloc(scratch_space);
                    continue;
                }

                let scratch_offsets = scratch_space.1.as_ref().unwrap();

                /* then transfer contents */
                for i in 0..nrecords {
                    let s = &stringsrc[stringdst.len() + i];
                    let start = scratch_offsets[i] as usize;
                    let end = if i + 1 < nrecords {
                        scratch_offsets[i + 1] as usize
                    } else {
                        nbytes
                    };
                    scratch_space.0[start..end].copy_from_slice(s.as_bytes())
                }

                (nrecords, nbytes)
            };

            /* then copy from scratch data to stringdst */
            let prev_len = stringdst.len();
            let input = InputData {
                data: Buffer::Borrowed(&scratch_space.0),
                cell_offsets: scratch_space
                    .1
                    .as_ref()
                    .map(|c| Buffer::Borrowed(c)),
            };
            stringdst
                .intermediate_result(nrecords, nbytes, input)
                .expect("Error aggregating Vec<String>");

            assert_eq!(nrecords, stringdst.len() - prev_len);
            assert_eq!(stringsrc[0..stringdst.len()], stringdst);
        }
    }

    proptest! {
        #[test]
        fn read_result_strings(record_capacity in MIN_RECORDS..=MAX_RECORDS, byte_capacity in MIN_BYTE_CAPACITY..=MAX_BYTE_CAPACITY, stringsrc in vec(any::<String>(), MIN_RECORDS..=MAX_RECORDS))
        {
            do_read_result_strings(record_capacity, byte_capacity, stringsrc)
        }
    }
}
