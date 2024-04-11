use super::*;

/// Query result handler which runs a callback on the results after each
/// step of execution.
pub struct CallbackReadQuery<'data, T, Q>
where
    T: DataReceiver,
{
    pub(crate) receiver: T,
    pub(crate) base: RawReadQuery<'data, T::Unit, Q>,
}

impl<'ctx, 'data, T, Q> ContextBound<'ctx> for CallbackReadQuery<'data, T, Q>
where
    Q: ContextBound<'ctx>,
    T: DataReceiver,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'data, T, Q> QueryCAPIInterface for CallbackReadQuery<'data, T, Q>
where
    Q: QueryCAPIInterface,
    T: DataReceiver,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data, T, Q> ReadQuery<'ctx> for CallbackReadQuery<'data, T, Q>
where
    T: DataReceiver,
    Q: ReadQuery<'ctx>,
{
    type Intermediate = Q::Intermediate;
    type Final = Q::Final;

    /// Run the query until it fills the scratch space.
    /// Invokes the callback on all data in the scratch space when the query returns.
    fn step(
        &mut self,
    ) -> TileDBResult<ReadStepOutput<Self::Intermediate, Self::Final>> {
        let base_result = self.base.step()?;

        let records_written =
            match self.base.raw_read_output.offsets_size.as_ref() {
                Some(offsets_size) => {
                    **offsets_size as usize / std::mem::size_of::<u64>()
                }
                None => {
                    *self.base.raw_read_output.data_size as usize
                        / std::mem::size_of::<<T as DataReceiver>::Unit>()
                }
            };
        let bytes_written = *self.base.raw_read_output.data_size as usize;

        let location = self.base.raw_read_output.location.borrow();

        /* TODO: check status and invoke callback with either borrowed or owned buffer */
        let input_data = InputData {
            data: Buffer::Borrowed(&*location.data),
            cell_offsets: location
                .cell_offsets
                .as_ref()
                .map(|c| Buffer::Borrowed(&*c)),
        };

        self.receiver
            .receive(records_written, bytes_written, input_data)?;

        Ok(match base_result {
            ReadStepOutput::NotEnoughSpace => ReadStepOutput::NotEnoughSpace,
            ReadStepOutput::Intermediate((_, _, base_result)) => {
                ReadStepOutput::Intermediate(base_result)
            }
            ReadStepOutput::Final((_, _, base_result)) => {
                ReadStepOutput::Final(base_result)
            }
        })
    }
}

pub struct CallbackReadBuilder<'data, T, B>
where
    T: DataReceiver,
{
    pub(crate) callback: T,
    pub(crate) base: RawReadBuilder<'data, <T as DataReceiver>::Unit, B>,
}

impl<'ctx, 'data, T, B> ContextBound<'ctx> for CallbackReadBuilder<'data, T, B>
where
    T: DataReceiver,
    B: ContextBound<'ctx>,
{
    fn context(&self) -> &'ctx Context {
        self.base.context()
    }
}

impl<'data, T, B> QueryCAPIInterface for CallbackReadBuilder<'data, T, B>
where
    T: DataReceiver,
    B: QueryCAPIInterface,
{
    fn raw(&self) -> &RawQuery {
        self.base.raw()
    }
}

impl<'ctx, 'data, T, B> QueryBuilder<'ctx> for CallbackReadBuilder<'data, T, B>
where
    T: DataReceiver,
    B: QueryBuilder<'ctx>,
{
    type Query = CallbackReadQuery<'data, T, B::Query>;

    fn array(&self) -> &Array {
        self.base.array()
    }

    fn build(self) -> Self::Query {
        CallbackReadQuery {
            receiver: self.callback,
            base: self.base.build(),
        }
    }
}

impl<'ctx, 'data, T, B> ReadQueryBuilder<'ctx>
    for CallbackReadBuilder<'data, T, B>
where
    T: DataReceiver,
    B: ReadQueryBuilder<'ctx>,
{
}
