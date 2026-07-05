# rsignals

A **one-shot, multi-signaler / multi-waiter, fail-safe broadcast event** for Rust.

## Why

`rsignals` exists to provide a **performant, reliable, and easy-to-use signal
service for multi-thread (and multi-task) coordination**.

The usual Rust one-shot primitives are **SPSC** — single producer, single
consumer: one `oneshot::Sender`, one `Receiver`, and cloning either side is not
part of the model. Real coordination problems are often wider than that: *any*
of several workers may be the one to report "ready" / "shutdown" / "found it",
and *many* threads or tasks need to hear about it at once.

`rsignals` is **MPMC by design**: any number of signalers, any number of
waiters, all sharing one event. The first signal wins and releases everyone —
with clear, mpsc-style errors on every edge (all signalers gone, all waiters
gone) so nothing ever blocks forever or fires into the void. Signaling is
wait-free, waiting has a lock-free fast path, and the whole crate is
dependency-free `std` with no `unsafe`.

One shared event, any number of senders and receivers. The first signal wins and
releases *every* waiter; late waiters observe the fired state immediately; and the
event is fail-safe on both sides — if all senders vanish, waiters are told; if all
receivers vanish, the sender is told.

Both a **blocking** (`rsignals::sync`) and a **runtime-agnostic async**
(`rsignals::r#async`) variant are provided, with identical semantics and error
types. Zero dependencies.

> The async module is spelled `r#async` because `async` is a Rust keyword —
> `use rsignals::r#async;` then `r#async::create()`.

## The model

An event is a tiny state machine with three states:

| State | Meaning |
| --- | --- |
| **Pending** | No signal yet, and at least one signaler still exists. |
| **Fired** | Some signaler called `signal()`; every waiter is released. |
| **Disconnected** | Every signaler was dropped without firing — no signal can ever arrive, so every waiter is released and told so. |

`Fired` and `Disconnected` are terminal and mutually exclusive: exactly one
transition out of `Pending` ever succeeds.

### Rules

- **One-shot:** the first `signal()` wins and returns `Ok(true)`; every later call
  returns `Ok(false)`.
- **Broadcast:** all current waiters wake; any waiter arriving after the event is
  already terminal returns immediately with the same outcome.
- **Fail-safe for waiters:** if the last signaler is dropped while pending, waiters
  are released with `Err(Disconnected)` instead of blocking forever.
- **Fail-safe for signalers:** if every waiter has been dropped, `signal()` returns
  `Err(NoReceivers)` and does **not** fire (there is no one to receive it).
- **Shared state:** handles are cheap to clone; every clone shares one underlying
  event. The state lives as long as any handle does.

## API at a glance

```text
create() -> (Signaler, Waiter)

Signaler::signal()          -> Result<bool, NoReceivers>   // Ok(true)=won, Ok(false)=already fired
Signaler::is_signaled()     -> bool
Signaler / Waiter :: signaler_count() / waiter_count() -> usize   // live-handle snapshots

// sync (blocking)
Waiter::wait()              -> Result<(), Disconnected>
Waiter::wait_timeout(dur)   -> Result<(), WaitTimeoutError>        // Timeout | Disconnected
Waiter::try_wait()          -> Result<(), TryWaitError>            // Pending | Disconnected

// future (async) — same as above, but:
Waiter::wait()              -> impl Future<Output = Result<(), Disconnected>>
// (no wait_timeout: wrap `wait()` with your runtime's timeout combinator)
```

The wait side mirrors [`std::sync::mpsc::Receiver`]: `Ok(())` means the event
fired, and each non-fired outcome has its own error variant so timeout, disconnect,
and still-pending are never conflated. `signal()` mirrors
[`std::sync::mpsc::Sender::send`], which fails once the receiver is gone.

[`std::sync::mpsc::Receiver`]: https://doc.rust-lang.org/std/sync/mpsc/struct.Receiver.html
[`std::sync::mpsc::Sender::send`]: https://doc.rust-lang.org/std/sync/mpsc/struct.Sender.html

## Usage

### Blocking

```rust
use std::thread;
use rsignals::sync::create;

let (tx, rx) = create();
let rx2 = rx.clone();

let waiter = thread::spawn(move || rx2.wait()); // blocks

assert_eq!(tx.signal(), Ok(true));          // first signal wins
assert_eq!(waiter.join().unwrap(), Ok(()));  // waiter released
assert_eq!(tx.signal(), Ok(false));          // one-shot: already fired
```

With a timeout:

```rust
use std::time::Duration;
use rsignals::sync::create;
use rsignals::WaitTimeoutError;

let (tx, rx) = create();
assert_eq!(rx.wait_timeout(Duration::ZERO), Err(WaitTimeoutError::Timeout));
tx.signal().unwrap();
assert_eq!(rx.wait_timeout(Duration::from_secs(1)), Ok(()));
```

### Async

Runtime-agnostic — works under Tokio, async-std, smol, or any executor:

```rust,ignore
let (tx, rx) = rsignals::r#async::create();

tokio::spawn(async move {
    match rx.wait().await {
        Ok(())              => println!("fired!"),
        Err(_disconnected)  => println!("all signalers gone"),
    }
});

tx.signal().unwrap();

// Timeouts are a runtime concern — compose with your executor:
// let _ = tokio::time::timeout(Duration::from_secs(1), rx.wait()).await;
```

### Fail-safe edges

```rust
use rsignals::sync::create;
use rsignals::{Disconnected, NoReceivers};

// All signalers dropped -> waiters learn the event will never fire.
let (tx, rx) = create();
drop(tx);
assert_eq!(rx.wait(), Err(Disconnected));

// All waiters dropped -> a signal has nowhere to go.
let (tx, rx) = create();
drop(rx);
assert_eq!(tx.signal(), Err(NoReceivers));
```

## Design & correctness

The full design rationale is in [`plan.md`](plan.md). Highlights:

- The `Pending -> {Fired, Disconnected}` transition is a single atomic
  compare-exchange (`Release` on success), so exactly one signaler — or the last
  signaler drop — wins. `signal()` is wait-free.
- **Sync:** a mutex + condvar handle blocking. The winner takes the mutex before
  `notify_all`, and waiters re-check the predicate under the mutex before sleeping
  — the standard handshake that avoids lost wakeups. `wait()` has a lock-free
  `Acquire` fast path.
- **Async:** parked tasks register their `Waker` in a mutex-guarded map; a terminal
  transition drains and wakes them *after* releasing the lock. `poll` re-checks the
  terminal state while holding the registry lock before parking (the async analog
  of the condvar predicate check), and a dropped `Wait` future deregisters its
  waker — so cancellation is clean and leak-free.
- Live-handle counts are maintained in each handle's `Clone`/`Drop`. The waiter
  count is *monotonic-to-zero* (a waiter can only be cloned from a live one), which
  is what makes `NoReceivers` reliable.

## Tests

```bash
cargo test        # unit, integration, doc-tests
cargo clippy --all-targets
```

Coverage includes: the full `plan.md` checklist, many-signaler/many-waiter races
under barriers, disconnect and no-receiver edges, a **model-based fuzzer**
(thousands of random op-sequences checked against a reference state machine),
randomized `wait_timeout` timing, and async tests (including manual-poll
waker-wiring and future cancellation) driven by a tiny dependency-free `block_on`.

## License

Apache-2.0 — see [LICENSE](LICENSE).
