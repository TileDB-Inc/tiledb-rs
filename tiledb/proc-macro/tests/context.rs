#[macro_use]
extern crate tiledb_proc_macro;

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

struct Context {}

impl Context {
    pub fn new() -> Self {
        Context {}
    }
}

trait ContextBound<'ctx> {
    fn context(&self) -> &'ctx Context;
}

#[derive(ContextBound)]
struct SimpleThing<'ctx> {
    #[context]
    context: &'ctx Context,
}

#[test]
fn simple() {
    let context = Context::new();
    let simple = SimpleThing { context: &context };

    assert_eq!(
        &context as *const Context,
        simple.context() as *const Context
    );
}

#[derive(Clone)]
struct DeriveBase<'ctx> {
    found: RefCell<bool>,
    context: &'ctx Context,
}

impl<'ctx> DeriveBase<'ctx> {
    fn new(context: &'ctx Context) -> Self {
        DeriveBase {
            found: RefCell::new(false),
            context,
        }
    }
}

impl<'ctx> ContextBound<'ctx> for DeriveBase<'ctx> {
    fn context(&self) -> &'ctx Context {
        *self.found.borrow_mut() = true;
        self.context
    }
}

#[derive(ContextBound)]
struct DirectBase<'ctx> {
    #[base(ContextBound)]
    base: DeriveBase<'ctx>,
}

#[test]
fn direct_base() {
    let context = Context::new();

    let s = DirectBase {
        base: DeriveBase::new(&context),
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.found.borrow());
}

#[derive(ContextBound)]
struct IndirectBase<'ctx> {
    #[base(ContextBound)]
    base: DirectBase<'ctx>,
}

#[test]
fn indirect_base() {
    let context = Context::new();

    let s = IndirectBase {
        base: DirectBase {
            base: DeriveBase::new(&context),
        },
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.base.found.borrow());
}

#[derive(ContextBound)]
struct GenericDirectBase<'ctx, T> {
    _marker: std::marker::PhantomData<T>,
    #[base(ContextBound)]
    base: DeriveBase<'ctx>,
}

#[test]
fn generic_direct_base() {
    let context = Context::new();

    let s = GenericDirectBase {
        _marker: std::marker::PhantomData::<u64>,
        base: DeriveBase::new(&context),
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.found.borrow());
}

#[derive(ContextBound)]
struct GenericIndirectBase<'ctx, T> {
    #[base(ContextBound)]
    base: GenericDirectBase<'ctx, T>,
}

#[test]
fn generic_indirect_base() {
    let context = Context::new();

    let s = GenericIndirectBase {
        base: GenericDirectBase {
            _marker: std::marker::PhantomData::<u64>,
            base: DeriveBase::new(&context),
        },
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.base.found.borrow());
}

#[derive(ContextBound)]
struct GenericDirectBaseBounded<'ctx, T>
where
    T: Default,
{
    _marker: std::marker::PhantomData<T>,
    #[base(ContextBound)]
    base: DeriveBase<'ctx>,
}

#[test]
fn generic_direct_base_bounded() {
    let context = Context::new();

    let s = GenericDirectBaseBounded {
        _marker: std::marker::PhantomData::<u64>,
        base: DeriveBase::new(&context),
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.found.borrow());
}

#[derive(ContextBound)]
struct GenericIndirectBaseBounded<'ctx, T>
where
    T: Default,
{
    #[base(ContextBound)]
    base: GenericDirectBaseBounded<'ctx, T>,
}

#[test]
fn generic_indirect_base_bounded() {
    let context = Context::new();

    let s = GenericIndirectBaseBounded {
        base: GenericDirectBaseBounded {
            _marker: std::marker::PhantomData::<u64>,
            base: DeriveBase::new(&context),
        },
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.base.found.borrow());
}

#[derive(ContextBound)]
struct UnboundedCtxBaseNotCtx<'ctx, T> {
    _marker: std::marker::PhantomData<&'ctx u64>,
    #[base(ContextBound)]
    base: T,
}

#[test]
fn unbounded_ctx_base_not_ctx() {
    let context = Context::new();

    let s = UnboundedCtxBaseNotCtx {
        _marker: std::marker::PhantomData,
        base: DeriveBase::new(&context),
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.found.borrow());
}

#[derive(ContextBound)]
struct ContextBoundBase<'ctx, T>
where
    T: ContextBound<'ctx>,
{
    _marker: std::marker::PhantomData<&'ctx u64>,
    #[base(ContextBound)]
    base: T,
}

#[test]
fn context_bound_base() {
    let context = Context::new();

    let s = ContextBoundBase {
        _marker: std::marker::PhantomData,
        base: DeriveBase::new(&context),
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.found.borrow());
}

#[derive(ContextBound)]
struct UnboundedBase<T> {
    #[base(ContextBound)]
    base: T,
}

#[test]
fn unbounded_base() {
    let context = Context::new();

    let s = UnboundedBase {
        base: DeriveBase::new(&context),
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.found.borrow());
}

#[derive(ContextBound)]
struct UnrelatedBoundedBase<T>
where
    T: Clone,
{
    #[base(ContextBound)]
    base: T,
}

#[test]
fn unrelated_bounded_base() {
    let context = Context::new();

    let s = UnrelatedBoundedBase {
        base: DeriveBase::new(&context),
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.found.borrow());
}

#[derive(ContextBound)]
struct BoxedBase<'ctx> {
    #[base(ContextBound)]
    base: Box<DeriveBase<'ctx>>,
}

#[test]
fn boxed_base() {
    let context = Context::new();

    let s = BoxedBase {
        base: Box::new(DeriveBase::new(&context)),
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.found.borrow());
}

#[derive(ContextBound)]
struct ArcBase<'ctx> {
    #[base(ContextBound)]
    base: Arc<DeriveBase<'ctx>>,
}

#[test]
fn arc_base() {
    let context = Context::new();

    let s = ArcBase {
        base: Arc::new(DeriveBase::new(&context)),
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.found.borrow());
}

#[derive(ContextBound)]
struct RcBase<'ctx> {
    #[base(ContextBound)]
    base: Rc<DeriveBase<'ctx>>,
}

#[test]
fn rc_base() {
    let context = Context::new();

    let s = RcBase {
        base: Rc::new(DeriveBase::new(&context)),
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.found.borrow());
}

#[derive(ContextBound)]
struct BoxUnboundedBase<T> {
    #[base(ContextBound)]
    base: Box<T>,
}

#[test]
fn box_unbounded_base() {
    let context = Context::new();

    let s = BoxedBase {
        base: Box::new(DeriveBase::new(&context)),
    };

    assert_eq!(&context as *const Context, s.context() as *const Context);
    assert!(*s.base.found.borrow());
}
