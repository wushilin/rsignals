//! Synchronous (blocking) one-shot broadcast event.
//!
//! Implementation follows the design in `plan.md`. The shared state is a small
//! state machine with three states:
//!
//! - `Pending`: no signal yet, and at least one signaler still exists,
//! - `Fired`: a signaler called `signal()`; every waiter is released,
//! - `Disconnected`: every signaler was dropped without firing, so no signal can
//!   ever arrive and every waiter is released and told so.
//!
//! `Fired` and `Disconnected` are both terminal and mutually exclusive: exactly
//! one successful compare-exchange moves `Pending` into one of them.
//!
//! The wait API mirrors [`std::sync::mpsc::Receiver`]: `Ok(())` means the event
//! fired, and the error type distinguishes disconnect (and, for the timed
//! variant, timeout).
//!
//! Synchronization mirrors `signal()`: the transition uses an atomic
//! compare-exchange (`Release` on success, `Relaxed` on failure), and the wait
//! path has a lock-free `Acquire` fast path. The mutex + condvar handle only the
//! park/notify handshake; whoever performs a terminal transition takes the mutex
//! before `notify_all`, which is what prevents a lost wakeup.

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

#[doc(inline)]
pub use crate::error::{Disconnected, NoReceivers, TryWaitError, WaitTimeoutError};

const PENDING: u8 = 0;
const FIRED: u8 = 1;
const DISCONNECTED: u8 = 2;

/// Internal terminal state.
#[derive(Clone, Copy)]
enum Terminal {
    Fired,
    Disconnected,
}

/// The shared, reference-counted state behind a signaler/waiter pair.
struct Shared {
    /// One of `PENDING` / `FIRED` / `DISCONNECTED`.
    state: AtomicU8,
    /// Number of live `Signaler` handles. When it reaches zero while still
    /// `PENDING`, the event transitions to `Disconnected`.
    signalers: AtomicUsize,
    /// Number of live `Waiter` handles. Monotonic-to-zero: once it reaches zero
    /// no waiter can ever observe a signal, so `signal()` reports `NoReceivers`.
    waiters: AtomicUsize,
    /// Guards the condvar park/notify handshake. Protects no data of its own.
    mutex: Mutex<()>,
    /// Wakes parked waiters on any terminal transition.
    condvar: Condvar,
}

impl Shared {
    fn new() -> Self {
        Shared {
            state: AtomicU8::new(PENDING),
            signalers: AtomicUsize::new(1), // the signaler returned by create()
            waiters: AtomicUsize::new(1),   // the waiter returned by create()
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
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

    /// Read the current state as a terminal outcome, or `None` if still pending.
    #[inline]
    fn load_terminal(&self) -> Option<Terminal> {
        match self.state.load(Ordering::Acquire) {
            FIRED => Some(Terminal::Fired),
            DISCONNECTED => Some(Terminal::Disconnected),
            _ => None,
        }
    }

    /// Fire the event exactly once.
    ///
    /// - `Ok(true)`: this caller won the one-shot transition,
    /// - `Ok(false)`: it was already fired,
    /// - `Err(NoReceivers)`: no waiter can ever observe it, so it is left unfired.
    fn signal(&self) -> Result<bool, NoReceivers> {
        // If it already fired (necessarily while a receiver existed), report that
        // regardless of the current receiver count.
        if self.state.load(Ordering::Acquire) == FIRED {
            return Ok(false);
        }
        // Not yet fired. If every waiter is gone (monotonic-to-zero, so this is
        // stable) the signal can reach no one; refuse rather than fire uselessly.
        // The tiny window where a waiter drops concurrently is best-effort, the
        // same inherent race as `mpsc::Sender::send` losing to a receiver drop.
        if self.waiters.load(Ordering::Acquire) == 0 {
            return Err(NoReceivers);
        }
        // Try to win. Because the caller holds a live Signaler the state cannot
        // be DISCONNECTED, so a failed CAS means another signaler just fired.
        // `Release` publishes prior writes to any waiter that observes FIRED with
        // `Acquire`; failure needs no ordering.
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

    /// Called when a `Signaler` is dropped. If it was the last one and the event
    /// never fired, transition PENDING -> DISCONNECTED and release all waiters.
    fn signaler_dropped(&self) {
        // `AcqRel`: this decrement must be ordered after every other signaler's
        // activity so that the last dropper reliably observes the count hit zero.
        if self.signalers.fetch_sub(1, Ordering::AcqRel) != 1 {
            return; // other signalers remain
        }
        // We were the last signaler. Only disconnect if nobody fired first;
        // a failed CAS means the state is already FIRED, which we must not undo.
        if self
            .state
            .compare_exchange(PENDING, DISCONNECTED, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            self.wake_all();
        }
    }

    /// Take the mutex, then notify. The acquisition — not the notify position —
    /// is what guarantees a waiter that read PENDING has finished parking.
    fn wake_all(&self) {
        let _guard = Self::lock_unpoisoned(&self.mutex);
        self.condvar.notify_all();
    }

    fn lock_unpoisoned(mutex: &Mutex<()>) -> MutexGuard<'_, ()> {
        match mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn wait_unpoisoned<'a>(&self, guard: MutexGuard<'a, ()>) -> MutexGuard<'a, ()> {
        match self.condvar.wait(guard) {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn wait_timeout_unpoisoned<'a>(
        &self,
        guard: MutexGuard<'a, ()>,
        timeout: Duration,
    ) -> (MutexGuard<'a, ()>, bool) {
        match self.condvar.wait_timeout(guard, timeout) {
            Ok((guard, result)) => (guard, result.timed_out()),
            Err(poisoned) => {
                let (guard, result) = poisoned.into_inner();
                (guard, result.timed_out())
            }
        }
    }

    /// Block until a terminal state, returning which one.
    fn wait(&self) -> Terminal {
        if let Some(t) = self.load_terminal() {
            return t;
        }
        let mut guard = Self::lock_unpoisoned(&self.mutex);
        loop {
            if let Some(t) = self.load_terminal() {
                return t;
            }
            guard = self.wait_unpoisoned(guard);
        }
    }

    /// Block until terminal or `timeout` elapses. `None` means it timed out.
    fn wait_timeout(&self, timeout: Duration) -> Option<Terminal> {
        if let Some(t) = self.load_terminal() {
            return Some(t);
        }
        let deadline = Instant::now().checked_add(timeout);
        let mut guard = Self::lock_unpoisoned(&self.mutex);
        loop {
            if let Some(t) = self.load_terminal() {
                return Some(t);
            }
            let remaining = match deadline {
                Some(d) => d.saturating_duration_since(Instant::now()),
                // Overflowing deadline: treat as effectively unbounded.
                None => {
                    guard = self.wait_unpoisoned(guard);
                    continue;
                }
            };
            if remaining.is_zero() {
                return None;
            }
            let (g, timed_out) = self.wait_timeout_unpoisoned(guard, remaining);
            guard = g;
            if timed_out {
                return self.load_terminal();
            }
        }
    }
}

/// A handle used to fire the event. Cheap to clone; all clones share state.
///
/// Dropping the last `Signaler` while the event is still pending transitions it
/// to *disconnected*, releasing every waiter with [`Disconnected`].
pub struct Signaler {
    shared: Arc<Shared>,
}

/// A handle used to wait for the event. Cheap to clone; all clones share state.
///
/// Dropping the last `Waiter` makes [`Signaler::signal`] report [`NoReceivers`].
pub struct Waiter {
    shared: Arc<Shared>,
}

/// Create a fresh, unfired event, returning a signaler and a waiter that share it.
///
/// Clone the returned handles to add more signalers/waiters; every clone shares
/// the same one-shot state.
///
/// ```
/// let (tx, rx) = rsignal::sync::create();
/// assert!(!rx.is_signaled());
/// assert_eq!(tx.signal(), Ok(true));
/// assert_eq!(rx.wait(), Ok(()));
/// ```
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
    /// Attempt to fire the event.
    ///
    /// - `Ok(true)`: this call fired it (the first successful signal wins),
    /// - `Ok(false)`: it was already fired by another signaler,
    /// - `Err(NoReceivers)`: every [`Waiter`] was dropped, so nobody can receive
    ///   the signal; the event is left unfired.
    ///
    /// ```
    /// use rsignal::sync::create;
    /// use rsignal::NoReceivers;
    ///
    /// let (tx, rx) = create();
    /// assert_eq!(tx.signal(), Ok(true));  // first wins
    /// assert_eq!(tx.signal(), Ok(false)); // one-shot: already fired
    ///
    /// let (tx2, rx2) = create();
    /// drop(rx2);                                 // nobody is listening
    /// assert_eq!(tx2.signal(), Err(NoReceivers));
    /// assert!(!tx2.is_signaled());               // and it did not fire
    /// # let _ = rx;
    /// ```
    pub fn signal(&self) -> Result<bool, NoReceivers> {
        self.shared.signal()
    }

    /// Whether the event has already fired.
    pub fn is_signaled(&self) -> bool {
        self.shared.state.load(Ordering::Acquire) == FIRED
    }

    /// Number of live [`Signaler`] handles sharing this event (a snapshot).
    pub fn signaler_count(&self) -> usize {
        self.shared.signaler_count()
    }

    /// Number of live [`Waiter`] handles sharing this event (a snapshot).
    pub fn waiter_count(&self) -> usize {
        self.shared.waiter_count()
    }
}

impl Clone for Signaler {
    fn clone(&self) -> Self {
        // Register another live signaler. `Relaxed` suffices: the cloning thread
        // already holds a live handle, so the count cannot be racing toward zero.
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
    /// Block until the event fires. `Ok(())` means it fired;
    /// `Err(Disconnected)` means every signaler was dropped without firing.
    pub fn wait(&self) -> Result<(), Disconnected> {
        match self.shared.wait() {
            Terminal::Fired => Ok(()),
            Terminal::Disconnected => Err(Disconnected),
        }
    }

    /// Block until fired, disconnected, or `timeout` elapses.
    ///
    /// Returns `Ok(())` if it fired, `Err(WaitTimeoutError::Disconnected)` if
    /// every signaler was dropped, or `Err(WaitTimeoutError::Timeout)` if the
    /// deadline passed first. A zero timeout makes it a non-blocking check.
    ///
    /// ```
    /// use std::time::Duration;
    /// use rsignal::sync::create;
    /// use rsignal::WaitTimeoutError;
    ///
    /// let (tx, rx) = create();
    /// assert_eq!(rx.wait_timeout(Duration::ZERO), Err(WaitTimeoutError::Timeout));
    /// assert_eq!(tx.signal(), Ok(true));
    /// assert_eq!(rx.wait_timeout(Duration::from_secs(1)), Ok(()));
    /// ```
    pub fn wait_timeout(&self, timeout: Duration) -> Result<(), WaitTimeoutError> {
        match self.shared.wait_timeout(timeout) {
            Some(Terminal::Fired) => Ok(()),
            Some(Terminal::Disconnected) => Err(WaitTimeoutError::Disconnected),
            None => Err(WaitTimeoutError::Timeout),
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

    /// Number of live [`Signaler`] handles sharing this event (a snapshot).
    pub fn signaler_count(&self) -> usize {
        self.shared.signaler_count()
    }

    /// Number of live [`Waiter`] handles sharing this event (a snapshot).
    pub fn waiter_count(&self) -> usize {
        self.shared.waiter_count()
    }
}

impl Clone for Waiter {
    fn clone(&self) -> Self {
        // Register another live receiver. `Relaxed` suffices: the cloning thread
        // already holds a live handle, so the count cannot be racing to zero.
        self.shared.waiters.fetch_add(1, Ordering::Relaxed);
        Waiter {
            shared: self.shared.clone(),
        }
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        // No wake needed: signalers never block on the receiver count.
        self.shared.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}
