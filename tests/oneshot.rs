//! Behavioral tests for the one-shot broadcast event.
//!
//! These map onto the "Test checklist" section of `plan.md`, plus:
//! - `Disconnected` (all signalers dropped without firing),
//! - `NoReceivers`  (all waiters dropped, so a signal can reach no one),
//! - sibling-count introspection,
//! - a heavily interleaved stress test with many signalers and waiters.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rsignal::sync::{create, Disconnected, NoReceivers, TryWaitError, WaitTimeoutError, Waiter};

const MANY: usize = 16; // "at least 10" waiters / signalers
const WATCHDOG: Duration = Duration::from_secs(5);

/// Run `rx.wait()` on a helper thread and return its result, failing cleanly if
/// the waiter does not return within the watchdog window (i.e. blocked forever)
/// instead of hanging the whole test binary.
fn wait_within(rx: Waiter) -> Result<(), Disconnected> {
    let (tx, done) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(rx.wait());
    });
    done.recv_timeout(WATCHDOG)
        .expect("waiter never returned (blocked forever?)")
}

/// 1. `wait()` called before `signal()` blocks, then returns after the signal.
#[test]
fn wait_before_signal_blocks_then_returns() {
    let (tx, rx) = create();
    let start = Instant::now();

    let waiter = thread::spawn(move || {
        let r = rx.wait();
        (r, start.elapsed())
    });

    thread::sleep(Duration::from_millis(100));
    assert_eq!(tx.signal(), Ok(true), "first signal must win");

    let (result, waited) = waiter.join().unwrap();
    assert_eq!(result, Ok(()));
    assert!(
        waited >= Duration::from_millis(90),
        "waiter returned too early ({waited:?}); it should have blocked until signal"
    );
}

/// 2. `signal()` before `wait()` makes a later wait return immediately.
#[test]
fn signal_before_wait_returns_immediately() {
    let (tx, rx) = create();
    assert_eq!(tx.signal(), Ok(true));

    let start = Instant::now();
    assert_eq!(rx.wait(), Ok(()));
    assert!(
        start.elapsed() < Duration::from_millis(50),
        "wait() after fire should be effectively instant"
    );
}

/// 3. Many waiters are all released by one successful signal.
#[test]
fn many_waiters_all_released_by_one_signal() {
    let (tx, rx) = create();
    let barrier = Arc::new(Barrier::new(MANY + 1));
    let released = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..MANY)
        .map(|_| {
            let rx = rx.clone();
            let barrier = barrier.clone();
            let released = released.clone();
            thread::spawn(move || {
                barrier.wait();
                if rx.wait().is_ok() {
                    released.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    barrier.wait();
    thread::sleep(Duration::from_millis(50));
    assert_eq!(tx.signal(), Ok(true));

    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(released.load(Ordering::SeqCst), MANY);
}

/// 4. Many concurrent signalers produce exactly one `Ok(true)`; others `Ok(false)`.
#[test]
fn many_concurrent_signalers_exactly_one_wins() {
    let (tx, _rx) = create(); // keep a receiver alive so signals are deliverable
    let barrier = Arc::new(Barrier::new(MANY));
    let wins = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..MANY)
        .map(|_| {
            let tx = tx.clone();
            let barrier = barrier.clone();
            let wins = wins.clone();
            thread::spawn(move || {
                barrier.wait();
                if tx.signal() == Ok(true) {
                    wins.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(wins.load(Ordering::SeqCst), 1, "exactly one signal must win");
}

/// 5. Repeated `signal()` calls after Fired always return `Ok(false)`.
#[test]
fn repeated_signal_after_fired_is_false() {
    let (tx, _rx) = create();
    assert_eq!(tx.signal(), Ok(true), "first wins");
    for _ in 0..MANY {
        assert_eq!(tx.signal(), Ok(false), "every later signal must fail");
    }
}

/// 6. Cloned signal handles share the same fired state.
#[test]
fn cloned_signalers_share_state() {
    let (tx, _rx) = create();
    let tx2 = tx.clone();
    assert_eq!(tx.signal(), Ok(true));
    assert_eq!(tx2.signal(), Ok(false), "clone sees event as already fired");
    assert!(tx2.is_signaled());
}

/// 7. Cloned wait handles observe the same fired state.
#[test]
fn cloned_waiters_observe_state() {
    let (tx, rx) = create();
    let rx2 = rx.clone();
    assert!(!rx.is_signaled());
    assert_eq!(tx.signal(), Ok(true));
    assert!(rx.is_signaled());
    assert!(rx2.is_signaled());
    assert_eq!(rx2.wait(), Ok(()));
}

/// 8. Repeated waits after Fired return immediately.
#[test]
fn repeated_waits_after_fired_return_immediately() {
    let (tx, rx) = create();
    assert_eq!(tx.signal(), Ok(true));
    let start = Instant::now();
    for _ in 0..MANY {
        assert_eq!(rx.wait(), Ok(()));
    }
    assert!(start.elapsed() < Duration::from_millis(50));
}

/// 9. `is_signaled()` reflects the transition Pending -> Fired.
#[test]
fn is_signaled_reflects_state() {
    let (tx, rx) = create();
    assert!(!tx.is_signaled());
    assert!(!rx.is_signaled());
    assert_eq!(tx.signal(), Ok(true));
    assert!(tx.is_signaled());
    assert!(rx.is_signaled());
}

/// 10. `wait_timeout` times out before the fire, and reports the fire after.
#[test]
fn wait_timeout_times_out_then_signals() {
    let (tx, rx) = create();

    let start = Instant::now();
    assert_eq!(
        rx.wait_timeout(Duration::from_millis(80)),
        Err(WaitTimeoutError::Timeout)
    );
    assert!(
        start.elapsed() >= Duration::from_millis(70),
        "wait_timeout returned before its deadline"
    );

    assert_eq!(tx.signal(), Ok(true));
    assert_eq!(rx.wait_timeout(Duration::from_millis(80)), Ok(()));
}

/// 11. `try_wait` is a non-blocking snapshot of the state.
#[test]
fn try_wait_is_nonblocking_snapshot() {
    let (tx, rx) = create();
    assert_eq!(rx.try_wait(), Err(TryWaitError::Pending));
    assert_eq!(tx.signal(), Ok(true));
    assert_eq!(rx.try_wait(), Ok(()));
}

/// 12. Dropping one handle does not invalidate shared state for the others.
#[test]
fn dropping_one_handle_keeps_state_alive() {
    let (tx, rx) = create();
    let tx2 = tx.clone();
    let rx2 = rx.clone();
    drop(tx);
    drop(rx);

    assert_eq!(tx2.signal(), Ok(true), "surviving signaler still fires");
    assert!(rx2.is_signaled(), "surviving waiter still observes it");
    assert_eq!(rx2.wait(), Ok(()));
}

/// 13. Interleaved stress: many signalers race many waiters concurrently.
#[test]
fn interleaved_signalers_and_waiters() {
    for _round in 0..25 {
        let (tx, rx) = create();
        let gate = Arc::new(Barrier::new(MANY * 2));
        let wins = Arc::new(AtomicUsize::new(0));
        let woke = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();

        for _ in 0..MANY {
            let tx = tx.clone();
            let gate = gate.clone();
            let wins = wins.clone();
            handles.push(thread::spawn(move || {
                gate.wait();
                if tx.signal() == Ok(true) {
                    wins.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for _ in 0..MANY {
            let rx = rx.clone();
            let gate = gate.clone();
            let woke = woke.clone();
            handles.push(thread::spawn(move || {
                gate.wait();
                if rx.wait().is_ok() {
                    woke.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(wins.load(Ordering::SeqCst), 1, "exactly one signal wins");
        assert_eq!(woke.load(Ordering::SeqCst), MANY, "every waiter released");
        assert!(rx.is_signaled());
    }
}

// ---------------------------------------------------------------------------
// Disconnected: all signalers dropped without firing.
// ---------------------------------------------------------------------------

/// 14. Dropping the only signaler without firing disconnects the waiter.
#[test]
fn dropping_all_signalers_disconnects_waiter() {
    let (tx, rx) = create();
    drop(tx);
    assert_eq!(wait_within(rx.clone()), Err(Disconnected));
    assert!(rx.is_disconnected());
    assert!(!rx.is_signaled());
}

/// 15. `is_disconnected` / `try_wait` / `wait_timeout` observe the disconnect.
#[test]
fn disconnected_state_is_observable() {
    let (tx, rx) = create();
    assert!(!rx.is_disconnected());
    assert_eq!(rx.try_wait(), Err(TryWaitError::Pending));

    drop(tx);

    assert!(rx.is_disconnected());
    assert_eq!(rx.try_wait(), Err(TryWaitError::Disconnected));
    assert_eq!(
        rx.wait_timeout(Duration::from_millis(10)),
        Err(WaitTimeoutError::Disconnected)
    );
}

/// 16. A waiter is already blocked when the last signaler is dropped. It must
///     wake up and learn the event is disconnected.
#[test]
fn blocked_waiter_wakes_on_disconnect() {
    let (tx, rx) = create();

    let (send, recv) = mpsc::channel();
    thread::spawn(move || {
        let _ = send.send(rx.wait());
    });

    thread::sleep(Duration::from_millis(100));
    drop(tx);

    let result = recv
        .recv_timeout(WATCHDOG)
        .expect("blocked waiter was never released by the disconnect");
    assert_eq!(result, Err(Disconnected));
}

/// 17. Disconnect is broadcast: many blocked waiters are all released.
#[test]
fn many_waiters_all_released_on_disconnect() {
    let (tx, rx) = create();
    let barrier = Arc::new(Barrier::new(MANY + 1));
    let (send, recv) = mpsc::channel();

    for _ in 0..MANY {
        let rx = rx.clone();
        let barrier = barrier.clone();
        let send = send.clone();
        thread::spawn(move || {
            barrier.wait();
            let _ = send.send(rx.wait());
        });
    }
    drop(send);

    barrier.wait();
    thread::sleep(Duration::from_millis(50));
    drop(tx);

    let mut disconnected = 0;
    for _ in 0..MANY {
        let r = recv
            .recv_timeout(WATCHDOG)
            .expect("a waiter was never released by the disconnect");
        assert_eq!(r, Err(Disconnected));
        disconnected += 1;
    }
    assert_eq!(disconnected, MANY);
}

/// 18. Only the *last* signaler drop disconnects; a survivor can still fire, and
///     firing wins over a later drop (Fired never flips to Disconnected).
#[test]
fn disconnect_needs_all_signalers_and_fire_wins() {
    let (tx, rx) = create();
    let mut clones: Vec<_> = (0..MANY).map(|_| tx.clone()).collect();
    drop(tx);

    let survivor = clones.pop().unwrap();
    for c in clones {
        drop(c);
    }
    assert_eq!(
        rx.try_wait(),
        Err(TryWaitError::Pending),
        "still pending while one signaler lives"
    );

    assert_eq!(survivor.signal(), Ok(true));
    assert_eq!(rx.wait(), Ok(()));

    drop(survivor); // dropping after a fire must NOT flip Fired -> Disconnected
    assert!(rx.is_signaled());
    assert!(!rx.is_disconnected());
    assert_eq!(rx.wait(), Ok(()));
}

/// 19. Interleaved race between firing and disconnecting. Every waiter observes
///     the same single-valued terminal state, here `Ok` (a signal ran first).
#[test]
fn interleaved_signal_vs_disconnect() {
    for _round in 0..25 {
        let (tx, rx) = create();
        let signalers: Vec<_> = (0..MANY).map(|_| tx.clone()).collect();
        drop(tx);

        let gate = Arc::new(Barrier::new(MANY * 2));
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        for (i, s) in signalers.into_iter().enumerate() {
            let gate = gate.clone();
            handles.push(thread::spawn(move || {
                gate.wait();
                if i % 2 == 0 {
                    let _ = s.signal();
                }
                drop(s);
            }));
        }

        for _ in 0..MANY {
            let rx = rx.clone();
            let gate = gate.clone();
            let outcomes = outcomes.clone();
            handles.push(thread::spawn(move || {
                gate.wait();
                let r = rx.wait();
                outcomes.lock().unwrap().push(r);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let outcomes = outcomes.lock().unwrap();
        assert_eq!(outcomes.len(), MANY);
        assert!(
            outcomes.iter().all(|&r| r == outcomes[0]),
            "all waiters must observe the same single-valued terminal state"
        );
        assert_eq!(
            outcomes[0],
            Ok(()),
            "at least one signal ran before all signalers dropped"
        );
    }
}

// ---------------------------------------------------------------------------
// NoReceivers: all waiters dropped, so a signal can reach no one.
// ---------------------------------------------------------------------------

/// 20. Signaling with no receivers errors and does not fire the event.
#[test]
fn signal_with_no_receivers_errors_and_does_not_fire() {
    let (tx, rx) = create();
    drop(rx);

    assert_eq!(tx.signal(), Err(NoReceivers));
    assert!(!tx.is_signaled(), "must not fire when nobody can receive");
    assert_eq!(tx.signal(), Err(NoReceivers), "still errors on retry");
}

/// 21. NoReceivers only once *every* waiter is gone; a survivor keeps it live.
#[test]
fn signal_ok_while_any_receiver_alive() {
    let (tx, rx) = create();
    let rx2 = rx.clone();
    drop(rx);
    assert_eq!(tx.waiter_count(), 1);
    assert_eq!(tx.signal(), Ok(true), "one receiver still alive");
    let _ = rx2;
}

/// 22. Many waiters: NoReceivers only after all of them are dropped.
#[test]
fn signal_errors_after_all_waiters_dropped() {
    let (tx, rx) = create();
    let clones: Vec<_> = (0..MANY).map(|_| rx.clone()).collect();
    drop(rx);
    assert_eq!(tx.waiter_count(), MANY, "clones keep receivers alive");
    drop(clones);
    assert_eq!(tx.waiter_count(), 0);
    assert_eq!(tx.signal(), Err(NoReceivers));
}

/// 23. Already-fired takes precedence over NoReceivers.
#[test]
fn already_fired_takes_precedence_over_no_receivers() {
    let (tx, rx) = create();
    assert_eq!(tx.signal(), Ok(true)); // fired while a receiver existed
    drop(rx); // now no receivers
    assert_eq!(
        tx.signal(),
        Ok(false),
        "already fired, so report AlreadyFired, not NoReceivers"
    );
    assert!(tx.is_signaled());
}

/// 24. Concurrent signalers all see NoReceivers when receivers are gone first.
#[test]
fn concurrent_signalers_all_see_no_receivers() {
    let (tx, rx) = create();
    let signalers: Vec<_> = (0..MANY).map(|_| tx.clone()).collect();
    drop(tx);
    drop(rx); // deterministic: no receivers before any signaler runs

    let barrier = Arc::new(Barrier::new(MANY));
    let errs = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = signalers
        .into_iter()
        .map(|s| {
            let barrier = barrier.clone();
            let errs = errs.clone();
            thread::spawn(move || {
                barrier.wait();
                if s.signal() == Err(NoReceivers) {
                    errs.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(errs.load(Ordering::SeqCst), MANY);
}

// ---------------------------------------------------------------------------
// Sibling-count introspection.
// ---------------------------------------------------------------------------

/// 25. Both handle types report the same live signaler/waiter counts.
#[test]
fn handles_report_sibling_counts() {
    let (tx, rx) = create();
    assert_eq!(tx.signaler_count(), 1);
    assert_eq!(tx.waiter_count(), 1);
    assert_eq!(rx.signaler_count(), 1);
    assert_eq!(rx.waiter_count(), 1);

    let tx2 = tx.clone();
    let rx2 = rx.clone();
    let rx3 = rx.clone();

    // Both sides observe the same counts.
    assert_eq!(tx.signaler_count(), 2);
    assert_eq!(rx.signaler_count(), 2);
    assert_eq!(tx.waiter_count(), 3);
    assert_eq!(rx.waiter_count(), 3);

    drop(tx2);
    assert_eq!(rx.signaler_count(), 1);

    drop(rx2);
    drop(rx3);
    assert_eq!(tx.waiter_count(), 1);
}

/// 26. Counts reach zero on full drop, consistent with the disconnect transition.
#[test]
fn counts_reach_zero_on_full_drop() {
    let (tx, rx) = create();
    let tx2 = tx.clone();
    drop(tx);
    assert_eq!(rx.signaler_count(), 1);

    drop(tx2);
    assert_eq!(rx.signaler_count(), 0);
    assert!(rx.is_disconnected(), "last signaler gone -> disconnected");
}
