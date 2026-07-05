//! `rsignals` — a one-shot, multi-signaler / multi-waiter, fail-safe broadcast event.
//!
//! See `plan.md` and `README.md` for the full design. In short, an event has one
//! shared state with three outcomes:
//!
//! - **Pending** — no signal yet, and at least one signaler still exists,
//! - **Fired** — some signaler called `signal()`; every waiter is released,
//! - **Disconnected** — every signaler was dropped without firing, so no signal
//!   can ever arrive and every waiter is released and told so.
//!
//! Rules:
//!
//! - a signal is delivered at most once: the first `signal()` wins (`Ok(true)`),
//!   every later attempt reports `Ok(false)`,
//! - if every waiter has been dropped, `signal()` reports [`NoReceivers`] and
//!   does not fire (there is no one to receive it),
//! - a waiter that arrives after the event is already terminal returns
//!   immediately with the same outcome,
//! - handles are cheap to clone; every clone shares the same underlying state.
//!
//! # Variants
//!
//! - [`sync`] — blocking [`sync::Waiter::wait`], backed by a mutex + condvar.
//! - `r#async` — runtime-agnostic async `wait()`, backed by a
//!   [`std::task::Waker`] registry. (Spelled `r#async` because `async` is a
//!   keyword.)
//!
//! Both share the same [error types](crate::error) and the same one-shot
//! semantics; only the wait side differs (block vs. `.await`).
//!
//! # Example (sync)
//!
//! ```
//! use std::thread;
//!
//! let (tx, rx) = rsignals::sync::create();
//! let rx2 = rx.clone();
//!
//! let waiter = thread::spawn(move || rx2.wait());
//!
//! assert_eq!(tx.signal(), Ok(true)); // first signal wins
//! assert_eq!(waiter.join().unwrap(), Ok(())); // waiter is released
//! assert_eq!(tx.signal(), Ok(false)); // one-shot: later signals report already-fired
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod error;
pub mod sync;

#[path = "async.rs"]
pub mod r#async;

pub use error::{Disconnected, NoReceivers, TryWaitError, WaitTimeoutError};

/// Compile-and-run the `rust` code blocks in `README.md` as doc-tests, without
/// affecting the rendered crate docs.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDocTests;
