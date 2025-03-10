use std::pin::Pin;
use std::sync::Mutex;

pub use nix::sys::signal::{SigHandler, Signal};

/// On unix systems, overrides a signal handler within its scope.
/// On windows, does nothing.
pub struct SignalGuard {
    signo: Signal,
    restore_handler: SigHandler,
}

#[cfg(windows)]
impl SignalGuard {
    pub fn new(signo: Signal, handler: SigHandler) -> Self {
        SignalGuard {
            signo,
            signal_handler,
        }
    }
}

#[cfg(not(windows))]
impl SignalGuard {
    pub fn new(signo: Signal, handler: SigHandler) -> Self {
        let restore_handler =
            unsafe { nix::sys::signal::signal(signo, handler) }
                .expect("Error installing signal handler");

        SignalGuard {
            signo,
            restore_handler,
        }
    }
}

#[cfg(not(windows))]
impl Drop for SignalGuard {
    fn drop(&mut self) {
        unsafe {
            nix::sys::signal::signal(self.signo, self.restore_handler)
                .expect("Error restoring previous signal handler");
        }
    }
}

struct RawSignalCallback(*mut ());

impl<'a> From<&mut SignalCallback<'a>> for RawSignalCallback {
    fn from(value: &mut SignalCallback<'a>) -> Self {
        let ptr = value as *const SignalCallback<'a>;
        RawSignalCallback(unsafe {
            std::mem::transmute::<*const SignalCallback<'a>, *mut ()>(ptr)
        })
    }
}

unsafe impl Send for RawSignalCallback {}

/*
 * https://rust-lang.github.io/rust-clippy/master/index.html#/declare_interior_mutable_const
 * > A "non-constant" const item is a legacy way to supply an initialized
 * > value to downstream static items. In this case the use of const is legit,
 * > and this lint should be suppressed.
 */
#[allow(clippy::declare_interior_mutable_const)]
const SIGNAL_CALLBACK_DEFAULT: Mutex<Option<RawSignalCallback>> =
    Mutex::new(None);
static SIGNAL_CALLBACKS: [Mutex<Option<RawSignalCallback>>; 32] =
    [SIGNAL_CALLBACK_DEFAULT; 32];

/// Overrides a signal handler within its scope, running a callback when the signal is received.
pub struct SignalCallback<'a> {
    guard: SignalGuard,
    restore_callback: Option<RawSignalCallback>,
    action: Box<dyn FnMut() + 'a>,
}

extern "C" fn signal_callback_dispatch(signo: i32) {
    if let Some(handler) =
        SIGNAL_CALLBACKS[signo as usize].lock().unwrap().as_mut()
    {
        let action = &mut unsafe {
            &mut *std::mem::transmute::<*mut (), *mut SignalCallback<'static>>(
                handler.0,
            )
        }
        .action;
        action();
    }
}

impl<'a> SignalCallback<'a> {
    pub fn new<F>(signo: Signal, handler: F) -> Pin<Box<Self>>
    where
        F: FnMut() + 'a,
    {
        // probably should mask off the signal during the following non-atomic operations

        let guard = SignalGuard::new(
            signo,
            SigHandler::Handler(signal_callback_dispatch),
        );

        let mut callback = Box::pin(SignalCallback {
            guard,
            restore_callback: None,
            action: Box::new(handler),
        });
        let raw_callback = RawSignalCallback::from(callback.as_mut().get_mut());
        SIGNAL_CALLBACKS[signo as i32 as usize]
            .lock()
            .unwrap()
            .replace(raw_callback);

        callback
    }
}

impl Drop for SignalCallback<'_> {
    fn drop(&mut self) {
        // probably should mask off the signal around the non-atomic
        // operations including this and dropping the guard
        *SIGNAL_CALLBACKS[self.guard.signo as i32 as usize]
            .lock()
            .unwrap() = self.restore_callback.take();
    }
}

#[cfg(all(test, not(windows)))]
mod tests {
    use super::*;

    #[test]
    fn guard_simple() {
        // set handler to ignore for the test
        {
            let _ignore = SignalGuard::new(Signal::SIGINT, SigHandler::SigIgn);

            // this will end test if ignoring did not work
            nix::sys::signal::raise(Signal::SIGINT).unwrap();
        }

        // check that default was restored
        let default = unsafe {
            nix::sys::signal::signal(Signal::SIGINT, SigHandler::SigDfl)
        }
        .unwrap();
        assert_eq!(default, SigHandler::SigDfl);
    }

    #[test]
    fn callback_simple() {
        let mut value = 0;

        // set handler to ignore for the test
        let _ignore = SignalGuard::new(Signal::SIGINT, SigHandler::SigIgn);

        nix::sys::signal::raise(Signal::SIGINT).unwrap();
        assert_eq!(value, 0);

        // callback handler shall overwrite
        {
            let _cb = SignalCallback::new(Signal::SIGINT, || value += 1);
            nix::sys::signal::raise(Signal::SIGINT).unwrap();
        }
        assert_eq!(value, 1);

        // callback should have been unregistered, previous guard is used
        nix::sys::signal::raise(Signal::SIGINT).unwrap();
        assert_eq!(value, 1);
    }

    #[test]
    fn callback_nest() {
        let mut v1 = 0;
        let mut v2 = 0;

        // set handler to ignore for the test
        let _ignore = SignalGuard::new(Signal::SIGINT, SigHandler::SigIgn);

        nix::sys::signal::raise(Signal::SIGINT).unwrap();
        assert_eq!(v1, 0);
        assert_eq!(v2, 0);

        // callback handler shall overwrite
        {
            let _cb1 = SignalCallback::new(Signal::SIGINT, || v1 += 1);
            nix::sys::signal::raise(Signal::SIGINT).unwrap();

            // nested callback handler shall overwrite again
            {
                let _cb2 = SignalCallback::new(Signal::SIGINT, || v2 += 100);
                nix::sys::signal::raise(Signal::SIGINT).unwrap();
            }
            assert_eq!(v2, 100);

            // then the previous callback should have been restored
            nix::sys::signal::raise(Signal::SIGINT).unwrap();
        }
        assert_eq!(v1, 2);
        assert_eq!(v2, 100);

        // then the ignore handler should have been restored
        nix::sys::signal::raise(Signal::SIGINT).unwrap();
        assert_eq!(v1, 2);
        assert_eq!(v2, 100);
    }

    #[test]
    fn callback_multi() {
        let mut v1 = 0;
        let mut v2 = 0;

        // set handler to ignore for the test
        let _usr1 = SignalGuard::new(Signal::SIGUSR1, SigHandler::SigIgn);
        let _usr2 = SignalGuard::new(Signal::SIGUSR2, SigHandler::SigIgn);

        nix::sys::signal::raise(Signal::SIGUSR1).unwrap();
        assert_eq!(v1, 0);
        assert_eq!(v2, 0);

        nix::sys::signal::raise(Signal::SIGUSR2).unwrap();
        assert_eq!(v1, 0);
        assert_eq!(v2, 0);

        // register both but see only SIGUSR1
        {
            let _cb1 = SignalCallback::new(Signal::SIGUSR1, || v1 += 1);
            let _cb2 = SignalCallback::new(Signal::SIGUSR2, || v2 += 1);
            nix::sys::signal::raise(Signal::SIGUSR1).unwrap();
        }
        assert_eq!(v1, 1);
        assert_eq!(v2, 0);

        // register both but see only SIGUSR2
        {
            let _cb1 = SignalCallback::new(Signal::SIGUSR1, || v1 += 1);
            let _cb2 = SignalCallback::new(Signal::SIGUSR2, || v2 += 1);
            nix::sys::signal::raise(Signal::SIGUSR2).unwrap();
        }
        assert_eq!(v1, 1);
        assert_eq!(v2, 1);

        // register both and see both signals
        {
            let _cb1 = SignalCallback::new(Signal::SIGUSR1, || v1 += 1);
            let _cb2 = SignalCallback::new(Signal::SIGUSR2, || v2 += 1);
            nix::sys::signal::raise(Signal::SIGUSR1).unwrap();
            nix::sys::signal::raise(Signal::SIGUSR2).unwrap();
        }
        assert_eq!(v1, 2);
        assert_eq!(v2, 2);
    }
}
