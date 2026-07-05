//! Error and result types shared by the [`sync`](crate::sync) and
//! `r#async` variants of the one-shot event.
//!
//! The wait side mirrors [`std::sync::mpsc`]: a successful wait yields `Ok(())`
//! (the event fired), and each way a wait can *not* succeed has its own error so
//! outcomes are never conflated.

use std::fmt;

/// The event can no longer fire: every signaler was dropped without signaling.
///
/// Returned by the blocking `wait()` and the async `wait().await`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Disconnected;

/// Error from the blocking `wait_timeout()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitTimeoutError {
    /// The timeout elapsed before the event fired.
    Timeout,
    /// Every signaler was dropped without firing.
    Disconnected,
}

/// Error from the non-blocking `try_wait()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryWaitError {
    /// Still pending: no signal yet, and at least one signaler remains.
    Pending,
    /// Every signaler was dropped without firing.
    Disconnected,
}

/// No waiter can ever receive the signal: every waiter handle was dropped.
///
/// Returned by `signal()`, which does not fire the event in this case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoReceivers;

impl WaitTimeoutError {
    /// `true` if the wait ended because every signaler disconnected.
    #[inline]
    pub fn is_disconnected(self) -> bool {
        matches!(self, WaitTimeoutError::Disconnected)
    }

    /// `true` if the wait ended because the timeout elapsed.
    #[inline]
    pub fn is_timeout(self) -> bool {
        matches!(self, WaitTimeoutError::Timeout)
    }
}

impl TryWaitError {
    /// `true` if the event is simply not fired yet (signalers still live).
    #[inline]
    pub fn is_pending(self) -> bool {
        matches!(self, TryWaitError::Pending)
    }

    /// `true` if every signaler disconnected without firing.
    #[inline]
    pub fn is_disconnected(self) -> bool {
        matches!(self, TryWaitError::Disconnected)
    }
}

impl fmt::Display for Disconnected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("all signalers dropped without firing")
    }
}

impl fmt::Display for WaitTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WaitTimeoutError::Timeout => f.write_str("timed out before the event fired"),
            WaitTimeoutError::Disconnected => f.write_str("all signalers dropped without firing"),
        }
    }
}

impl fmt::Display for TryWaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryWaitError::Pending => f.write_str("event has not fired yet"),
            TryWaitError::Disconnected => f.write_str("all signalers dropped without firing"),
        }
    }
}

impl fmt::Display for NoReceivers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("all waiters dropped; no one can receive the signal")
    }
}

impl std::error::Error for Disconnected {}
impl std::error::Error for WaitTimeoutError {}
impl std::error::Error for TryWaitError {}
impl std::error::Error for NoReceivers {}

/// `Disconnected` converts into the richer timeout error for easy `?` bubbling.
impl From<Disconnected> for WaitTimeoutError {
    fn from(_: Disconnected) -> Self {
        WaitTimeoutError::Disconnected
    }
}

/// `Disconnected` converts into the richer try error for easy `?` bubbling.
impl From<Disconnected> for TryWaitError {
    fn from(_: Disconnected) -> Self {
        TryWaitError::Disconnected
    }
}
