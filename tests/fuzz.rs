//! Randomized ("fuzzy") and corner-case tests for the sync event, focused on
//! `try_wait` and `wait_timeout`.
//!
//! Randomness uses a tiny seeded xorshift PRNG so every failure is reproducible
//! from the printed seed — no external `rand` dependency.

use std::time::{Duration, Instant};
use std::{sync::Arc, thread};

use rsignal::sync::{create, Disconnected, NoReceivers, Signaler, TryWaitError, Waiter, WaitTimeoutError};

/// Seeded xorshift64* PRNG. Deterministic and dependency-free.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Avoid the all-zero state, which xorshift cannot escape.
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform-ish value in `[0, n)`.
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }
}

// ---------------------------------------------------------------------------
// Model-based fuzz: apply random operations and check every observable against
// an independent reference model of the state machine.
// ---------------------------------------------------------------------------

/// 2000 random op-sequences over the full API, each checked against a model.
#[test]
fn fuzz_state_machine_against_model() {
    for seed in 0..2000u64 {
        let mut rng = Rng::new(seed);
        let (tx0, rx0) = create();
        let mut sigs: Vec<Signaler> = vec![tx0];
        let mut waiters: Vec<Waiter> = vec![rx0];

        // Reference model.
        let mut fired = false;
        let mut disconnected = false;

        for _step in 0..40 {
            match rng.below(6) {
                0 => {
                    // clone a signaler
                    if let Some(s) = sigs.first() {
                        sigs.push(s.clone());
                    }
                }
                1 => {
                    // drop a signaler
                    if !sigs.is_empty() {
                        let i = rng.below(sigs.len() as u64) as usize;
                        sigs.remove(i);
                        if sigs.is_empty() && !fired {
                            disconnected = true;
                        }
                    }
                }
                2 => {
                    // clone a waiter
                    if let Some(w) = waiters.first() {
                        waiters.push(w.clone());
                    }
                }
                3 => {
                    // drop a waiter
                    if !waiters.is_empty() {
                        let i = rng.below(waiters.len() as u64) as usize;
                        waiters.remove(i);
                    }
                }
                4 => {
                    // signal
                    if let Some(s) = sigs.first() {
                        let expected = if fired {
                            Ok(false)
                        } else if waiters.is_empty() {
                            Err(NoReceivers)
                        } else {
                            fired = true;
                            Ok(true)
                        };
                        assert_eq!(s.signal(), expected, "seed {seed}: signal outcome");
                    }
                }
                _ => { /* no-op step, lets other random ops interleave */ }
            }

            // Model must be internally consistent.
            assert!(!(fired && disconnected), "seed {seed}: model invariant");

            // Check every live handle agrees with the model.
            if let Some(s) = sigs.first() {
                assert_eq!(s.is_signaled(), fired, "seed {seed}: signaler.is_signaled");
                assert_eq!(s.signaler_count(), sigs.len(), "seed {seed}: signaler_count");
                assert_eq!(s.waiter_count(), waiters.len(), "seed {seed}: waiter_count");
            }
            if let Some(w) = waiters.first() {
                assert_eq!(w.is_signaled(), fired, "seed {seed}: waiter.is_signaled");
                assert_eq!(w.is_disconnected(), disconnected, "seed {seed}: is_disconnected");
                let expected = if fired {
                    Ok(())
                } else if disconnected {
                    Err(TryWaitError::Disconnected)
                } else {
                    Err(TryWaitError::Pending)
                };
                assert_eq!(w.try_wait(), expected, "seed {seed}: try_wait");
                assert_eq!(w.signaler_count(), sigs.len(), "seed {seed}: w.signaler_count");
                assert_eq!(w.waiter_count(), waiters.len(), "seed {seed}: w.waiter_count");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Randomized timing fuzz for wait_timeout.
// ---------------------------------------------------------------------------

/// Random timeouts against three scenarios; asserts robust invariants.
#[test]
fn fuzz_wait_timeout_scenarios() {
    for seed in 0..300u64 {
        let mut rng = Rng::new(seed ^ 0xA5A5_A5A5);
        let (tx, rx) = create();
        let timeout = Duration::from_millis(rng.below(25));

        match rng.below(3) {
            0 => {
                // Fired before the wait -> always Ok, regardless of timeout (incl. 0).
                assert_eq!(tx.signal(), Ok(true));
                assert_eq!(rx.wait_timeout(timeout), Ok(()), "seed {seed}: fired-first");
            }
            1 => {
                // Disconnected before the wait -> always Disconnected.
                drop(tx);
                assert_eq!(
                    rx.wait_timeout(timeout),
                    Err(WaitTimeoutError::Disconnected),
                    "seed {seed}: disconnected-first"
                );
            }
            _ => {
                // Race: fire after a random delay while the signaler stays alive.
                let delay = Duration::from_millis(rng.below(25));
                let txc = tx.clone();
                let h = thread::spawn(move || {
                    thread::sleep(delay);
                    let _ = txc.signal();
                });

                let r = rx.wait_timeout(timeout);
                // Signaler alive -> never Disconnected; either it fired in time or timed out.
                assert!(
                    matches!(r, Ok(()) | Err(WaitTimeoutError::Timeout)),
                    "seed {seed}: race gave {r:?}"
                );

                h.join().unwrap();
                // The fire always lands eventually; no wakeup is ever lost.
                assert_eq!(rx.wait(), Ok(()), "seed {seed}: fire eventually observed");
                drop(tx);
            }
        }
    }
}

/// With a live signaler that never fires, wait_timeout must always time out and
/// must not return before its deadline.
#[test]
fn fuzz_wait_timeout_never_fires() {
    for seed in 0..200u64 {
        let mut rng = Rng::new(seed ^ 0x1234_5678);
        let (tx, rx) = create();
        let timeout = Duration::from_millis(5 + rng.below(15));

        let start = Instant::now();
        assert_eq!(
            rx.wait_timeout(timeout),
            Err(WaitTimeoutError::Timeout),
            "seed {seed}"
        );
        assert!(
            start.elapsed() + Duration::from_millis(2) >= timeout,
            "seed {seed}: returned before the deadline"
        );

        drop(tx); // keep the signaler alive across the wait
    }
}

/// Many waiters with random timeouts are all released by a single late signal;
/// none observe anything but Ok.
#[test]
fn fuzz_many_waiters_random_timeouts_all_released() {
    for seed in 0..60u64 {
        let mut rng = Rng::new(seed ^ 0xDEAD_BEEF);
        let (tx, rx) = create();
        let handles: Vec<_> = (0..12)
            .map(|_| {
                // Generous timeout so the signal always arrives first.
                let timeout = Duration::from_secs(5 + rng.below(3));
                let rx = rx.clone();
                thread::spawn(move || rx.wait_timeout(timeout))
            })
            .collect();

        thread::sleep(Duration::from_millis(10));
        assert_eq!(tx.signal(), Ok(true));

        for h in handles {
            assert_eq!(h.join().unwrap(), Ok(()), "seed {seed}: every waiter released");
        }
    }
}

// ---------------------------------------------------------------------------
// Deterministic corner cases.
// ---------------------------------------------------------------------------

/// Zero-duration timeout is a pure state snapshot: it never blocks.
#[test]
fn zero_timeout_snapshots_state() {
    let (tx, rx) = create();
    let start = Instant::now();
    assert_eq!(rx.wait_timeout(Duration::ZERO), Err(WaitTimeoutError::Timeout));
    assert!(start.elapsed() < Duration::from_millis(50), "zero timeout must not block");

    assert_eq!(tx.signal(), Ok(true));
    assert_eq!(rx.wait_timeout(Duration::ZERO), Ok(()));
}

/// Zero-duration timeout after disconnect reports Disconnected, not Timeout.
#[test]
fn zero_timeout_after_disconnect() {
    let (tx, rx) = create();
    drop(tx);
    assert_eq!(
        rx.wait_timeout(Duration::ZERO),
        Err(WaitTimeoutError::Disconnected)
    );
}

/// try_wait is monotonic: Pending... then Ok forever after a fire.
#[test]
fn try_wait_monotonic_to_fired() {
    let (tx, rx) = create();
    for _ in 0..5 {
        assert_eq!(rx.try_wait(), Err(TryWaitError::Pending));
    }
    assert_eq!(tx.signal(), Ok(true));
    for _ in 0..5 {
        assert_eq!(rx.try_wait(), Ok(()));
    }
}

/// try_wait is monotonic: Pending... then Disconnected forever after all drop.
#[test]
fn try_wait_monotonic_to_disconnected() {
    let (tx, rx) = create();
    assert_eq!(rx.try_wait(), Err(TryWaitError::Pending));
    drop(tx);
    for _ in 0..5 {
        assert_eq!(rx.try_wait(), Err(TryWaitError::Disconnected));
    }
}

/// A huge timeout returns immediately when the event has already fired.
#[test]
fn huge_timeout_returns_promptly_when_fired() {
    let (tx, rx) = create();
    assert_eq!(tx.signal(), Ok(true));
    let start = Instant::now();
    assert_eq!(rx.wait_timeout(Duration::from_secs(3600)), Ok(()));
    assert!(start.elapsed() < Duration::from_millis(50));
}

/// wait_timeout wakes on the signal well before a long deadline.
#[test]
fn wait_timeout_wakes_before_deadline() {
    let (tx, rx) = create();
    let h = thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        assert_eq!(tx.signal(), Ok(true));
    });
    let start = Instant::now();
    assert_eq!(rx.wait_timeout(Duration::from_secs(10)), Ok(()));
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "should wake on the signal, not wait the full 10s"
    );
    h.join().unwrap();
}

/// A waiter cloned after the fire immediately observes it.
#[test]
fn clone_after_fire_observes_fired() {
    let (tx, rx) = create();
    assert_eq!(tx.signal(), Ok(true));
    let rx2 = rx.clone();
    assert_eq!(rx2.try_wait(), Ok(()));
    assert_eq!(rx2.wait(), Ok(()));
    assert_eq!(rx2.wait_timeout(Duration::ZERO), Ok(()));
}

/// A waiter cloned after disconnect immediately observes the disconnect.
#[test]
fn clone_after_disconnect_observes_disconnected() {
    let (tx, rx) = create();
    drop(tx);
    let rx2 = rx.clone();
    assert!(rx2.is_disconnected());
    assert_eq!(rx2.wait(), Err(Disconnected));
    assert_eq!(rx2.try_wait(), Err(TryWaitError::Disconnected));
}

/// Shared `Arc` usage: many threads spin on try_wait across a single fire. Each
/// must eventually observe the fire (monotonic to `Ok`) and must never observe
/// `Disconnected` while a signaler is alive.
#[test]
fn concurrent_try_wait_around_fire() {
    let (tx, rx) = create();
    let rx = Arc::new(rx);
    let start = Instant::now();
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let rx = rx.clone();
            thread::spawn(move || loop {
                match rx.try_wait() {
                    Ok(()) => break, // observed the fire; done
                    Err(TryWaitError::Pending) => {}
                    Err(TryWaitError::Disconnected) => panic!("never disconnected here"),
                }
                assert!(start.elapsed() < Duration::from_secs(5), "fire never observed");
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(2));
    assert_eq!(tx.signal(), Ok(true));

    for h in handles {
        h.join().unwrap(); // returning at all means the fire was observed
    }
    assert_eq!(rx.try_wait(), Ok(()));
}
