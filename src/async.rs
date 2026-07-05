//! Asynchronous (`.await`-based) one-shot broadcast event.
//!
//! Same three-state machine and one-shot semantics as [`sync`](crate::sync); the
//! only difference is the wait side. [`Waiter::wait`] returns a [`Wait`] future
//! that resolves to `Ok(())` when the event fires or `Err(Disconnected)` when
//! every signaler is dropped first. Signaling itself is non-blocking, so
//! [`Signaler::signal`] stays a plain (non-async) method.
//!
//! This variant is **runtime-agnostic**: it depends only on `std` and the
//! [`std::task`] machinery, so it works under any executor (Tokio, async-std,
//! smol, a hand-rolled one, …). There is deliberately no `wait_timeout`: timeouts
//! are a runtime concern — wrap `wait()` with your executor's timeout combinator
//! (e.g. `tokio::time::timeout`).
//!
//! # Design notes
//!
//! - The `PENDING -> {FIRED, DISCONNECTED}` transition is the same once-only
//!   atomic compare-exchange as the sync version (`Release` on success).
//! - Parked tasks register their [`Waker`] in a mutex-guarded map. A terminal
//!   transition drains the map and wakes every waker **after** releasing the
//!   lock, so a woken task re-polling cannot deadlock on re-entry.
//! - `Wait::poll` re-checks the terminal state *while holding the registry lock*
//!   before parking — the async analog of checking the predicate under the
//!   condvar mutex — which is what prevents a lost wakeup.
//! - A `Wait` future deregisters its waker on drop, so cancelling a pending await
//!   leaves no stale entry behind.
//!
//! # Example
//!
//! ```
//! # // A tiny thread-parking `block_on` so this doc-test needs no runtime.
//! # use std::{future::Future, pin::pin, sync::Arc, task::{Context, Poll, Wake, Waker}, thread};
//! # struct W(thread::Thread);
//! # impl Wake for W { fn wake(self: Arc<Self>) { self.0.unpark() } fn wake_by_ref(self: &Arc<Self>) { self.0.unpark() } }
//! # fn block_on<F: Future>(f: F) -> F::Output {
//! #     let mut f = pin!(f);
//! #     let w = Waker::from(Arc::new(W(thread::current())));
//! #     let mut cx = Context::from_waker(&w);
//! #     loop { match f.as_mut().poll(&mut cx) { Poll::Ready(v) => return v, Poll::Pending => thread::park() } }
//! # }
//! let (tx, rx) = rsignal::r#async::create();
//! assert_eq!(tx.signal(), Ok(true));
//! assert_eq!(block_on(rx.wait()), Ok(()));
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

#[doc(inline)]
pub use crate::error::{Disconnected, NoReceivers, TryWaitError};

const PENDING: u8 = 0;
const FIRED: u8 = 1;
const DISCONNECTED: u8 = 2;

/// Internal terminal state.
#[derive(Clone, Copy)]
enum Terminal {
    Fired,
    Disconnected,
}

/// Waker registry: parked tasks keyed by a unique id so a re-poll updates (not
/// duplicates) its entry and a drop can remove exactly its own.
#[derive(Default)]
struct Registry {
    next_key: u64,
    wakers: HashMap<u64, Waker>,
}

/// The shared, reference-counted state behind a signaler/waiter pair.
struct Shared {
    /// One of `PENDING` / `FIRED` / `DISCONNECTED`.
    state: AtomicU8,
    /// Number of live `Signaler` handles (see [`sync`](crate::sync)).
    signalers: AtomicUsize,
    /// Number of live `Waiter` handles; monotonic-to-zero.
    waiters: AtomicUsize,
    /// Wakers of parked tasks. Also the mutex that serializes the register/notify
    /// handshake and prevents lost wakeups.
    registry: Mutex<Registry>,
}

impl Shared {
    fn new() -> Self {
        Shared {
            state: AtomicU8::new(PENDING),
            signalers: AtomicUsize::new(1),
            waiters: AtomicUsize::new(1),
            registry: Mutex::new(Registry::default()),
        }
    }

    #[inline]
    fn signaler_count(&self) -> usize {
        self.signalers.load(Ordering::Acquire)
    }

    #[inline]
    fn waiter_count(&self) -> usize {
        self.waiters.load(Ordering::Acquire)
    }

    #[inline]
    fn load_terminal(&self) -> Option<Terminal> {
        match self.state.load(Ordering::Acquire) {
            FIRED => Some(Terminal::Fired),
            DISCONNECTED => Some(Terminal::Disconnected),
            _ => None,
        }
    }

    /// Fire the event exactly once. Semantics identical to the sync variant.
    fn signal(&self) -> Result<bool, NoReceivers> {
        if self.state.load(Ordering::Acquire) == FIRED {
            return Ok(false);
        }
        if self.waiters.load(Ordering::Acquire) == 0 {
            return Err(NoReceivers);
        }
        match self
            .state
            .compare_exchange(PENDING, FIRED, Ordering::Release, Ordering::Relaxed)
        {
            Ok(_) => {
                self.wake_all();
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    /// Last-signaler-drop transition to `DISCONNECTED`, waking all waiters.
    fn signaler_dropped(&self) {
        if self.signalers.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        if self
            .state
            .compare_exchange(PENDING, DISCONNECTED, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            self.wake_all();
        }
    }

    /// Drain and wake every registered waker. Draining happens under the lock;
    /// the wakes happen after releasing it so a synchronous re-poll cannot
    /// deadlock by re-entering the registry.
    fn wake_all(&self) {
        let wakers: Vec<Waker> = {
            let mut reg = self.lock_registry();
            reg.wakers.drain().map(|(_, w)| w).collect()
        };
        for w in wakers {
            w.wake();
        }
    }

    fn lock_registry(&self) -> MutexGuard<'_, Registry> {
        match self.registry.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

/// A handle used to fire the event. Cheap to clone; all clones share state.
///
/// Dropping the last `Signaler` while still pending disconnects every waiter.
pub struct Signaler {
    shared: Arc<Shared>,
}

/// A handle used to await the event. Cheap to clone; all clones share state.
///
/// Dropping the last `Waiter` makes [`Signaler::signal`] report [`NoReceivers`].
pub struct Waiter {
    shared: Arc<Shared>,
}

/// Create a fresh, unfired async event, returning a signaler and a waiter.
pub fn create() -> (Signaler, Waiter) {
    let shared = Arc::new(Shared::new());
    (
        Signaler {
            shared: shared.clone(),
        },
        Waiter { shared },
    )
}

impl Signaler {
    /// Attempt to fire the event. `Ok(true)` wins, `Ok(false)` already fired,
    /// `Err(NoReceivers)` if every waiter was dropped (the event is left unfired).
    pub fn signal(&self) -> Result<bool, NoReceivers> {
        self.shared.signal()
    }

    /// Whether the event has already fired.
    pub fn is_signaled(&self) -> bool {
        self.shared.state.load(Ordering::Acquire) == FIRED
    }

    /// Number of live [`Signaler`] handles (a snapshot).
    pub fn signaler_count(&self) -> usize {
        self.shared.signaler_count()
    }

    /// Number of live [`Waiter`] handles (a snapshot).
    pub fn waiter_count(&self) -> usize {
        self.shared.waiter_count()
    }
}

impl Clone for Signaler {
    fn clone(&self) -> Self {
        self.shared.signalers.fetch_add(1, Ordering::Relaxed);
        Signaler {
            shared: self.shared.clone(),
        }
    }
}

impl Drop for Signaler {
    fn drop(&mut self) {
        self.shared.signaler_dropped();
    }
}

impl Waiter {
    /// Return a future that resolves when the event fires (`Ok(())`) or when
    /// every signaler is dropped first (`Err(Disconnected)`).
    pub fn wait(&self) -> Wait<'_> {
        Wait {
            waiter: self,
            key: None,
        }
    }

    /// Non-blocking snapshot. `Ok(())` if fired, `Err(Pending)` if still waiting,
    /// `Err(Disconnected)` if no signal can ever arrive.
    pub fn try_wait(&self) -> Result<(), TryWaitError> {
        match self.shared.load_terminal() {
            Some(Terminal::Fired) => Ok(()),
            Some(Terminal::Disconnected) => Err(TryWaitError::Disconnected),
            None => Err(TryWaitError::Pending),
        }
    }

    /// Whether the event has already fired.
    pub fn is_signaled(&self) -> bool {
        self.shared.state.load(Ordering::Acquire) == FIRED
    }

    /// Whether all signalers disconnected without firing.
    pub fn is_disconnected(&self) -> bool {
        self.shared.state.load(Ordering::Acquire) == DISCONNECTED
    }

    /// Number of live [`Signaler`] handles (a snapshot).
    pub fn signaler_count(&self) -> usize {
        self.shared.signaler_count()
    }

    /// Number of live [`Waiter`] handles (a snapshot).
    pub fn waiter_count(&self) -> usize {
        self.shared.waiter_count()
    }
}

impl Clone for Waiter {
    fn clone(&self) -> Self {
        self.shared.waiters.fetch_add(1, Ordering::Relaxed);
        Waiter {
            shared: self.shared.clone(),
        }
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        self.shared.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The future returned by [`Waiter::wait`].
///
/// Resolves to `Ok(())` on fire or `Err(Disconnected)` if every signaler is
/// dropped first. Dropping it before completion cancels the wait cleanly and
/// removes its registered waker.
pub struct Wait<'a> {
    waiter: &'a Waiter,
    /// This future's registry key, allocated on the first `Pending` poll.
    key: Option<u64>,
}

impl Future for Wait<'_> {
    type Output = Result<(), Disconnected>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut(); // `Wait` is `Unpin`
        let shared = &this.waiter.shared;

        // Lock-free fast path.
        match shared.load_terminal() {
            Some(Terminal::Fired) => return Poll::Ready(Ok(())),
            Some(Terminal::Disconnected) => return Poll::Ready(Err(Disconnected)),
            None => {}
        }

        // Slow path: register (or refresh) our waker under the registry lock, but
        // re-check the terminal state under that lock first, so a signal that
        // lands between the fast-path load and here cannot be missed.
        let mut reg = shared.lock_registry();
        match shared.load_terminal() {
            Some(Terminal::Fired) => return Poll::Ready(Ok(())),
            Some(Terminal::Disconnected) => return Poll::Ready(Err(Disconnected)),
            None => {}
        }

        let key = match this.key {
            Some(k) => k,
            None => {
                let k = reg.next_key;
                reg.next_key += 1;
                this.key = Some(k);
                k
            }
        };
        // Always store the *current* waker: a task may be polled by different
        // wakers over its lifetime (e.g. if moved between executors).
        reg.wakers.insert(key, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for Wait<'_> {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            // Deregister so a cancelled wait leaves no stale waker behind.
            let mut reg = self.waiter.shared.lock_registry();
            reg.wakers.remove(&key);
        }
    }
}
