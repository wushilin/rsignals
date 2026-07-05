//! Opt-in many-to-many stress/perf tests.
//!
//! These are ignored by default because the large cases intentionally allocate a
//! lot of memory and, for sync waits, may spawn many OS threads. Run with:
//!
//! ```text
//! cargo test --test many_to_many -- --ignored --nocapture
//! ```
//!
//! Tune sizes with:
//! - `RSIGNAL_STRESS_WAITERS`
//! - `RSIGNAL_STRESS_SIGNALERS`
//! - `RSIGNAL_STRESS_SYNC_BLOCKING_WAITERS`
//! - `RSIGNAL_STRESS_SYNC_BLOCKING_SIGNALERS`
//!
//! The async test can exercise one million registered pending waiters on one
//! thread. The sync blocking test defaults much lower because one blocked sync
//! waiter is one native thread.

use std::future::Future;
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use rsignal::{r#async as async_signal, sync};

const DEFAULT_LARGE: usize = 1_000_000;
const DEFAULT_SYNC_BLOCKING_WAITERS: usize = 1_000;
const DEFAULT_SYNC_BLOCKING_SIGNALERS: usize = 1_000;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?;
        value.split_whitespace().next()?.parse().ok()
    })
}

fn hwm_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmHWM:")?;
        value.split_whitespace().next()?.parse().ok()
    })
}

fn print_stats(label: &str, waiters: usize, signalers: usize, started: Instant, woke: usize) {
    println!(
        "{label}: waiters={waiters}, signalers={signalers}, woke={woke}, elapsed={:?}, rss={} KiB, peak_rss={} KiB",
        started.elapsed(),
        rss_kib().unwrap_or(0),
        hwm_kib().unwrap_or(0),
    );
}

struct ThreadWaker(thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => thread::park(),
        }
    }
}

struct LatencyWaker {
    wakes: Arc<AtomicUsize>,
    max_wake_ns: Arc<AtomicU64>,
    start: Arc<OnceLock<Instant>>,
}

impl Wake for LatencyWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        let elapsed = self
            .start
            .get()
            .expect("signal start time must be recorded before wake")
            .elapsed()
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        let mut current = self.max_wake_ns.load(Ordering::Relaxed);
        while elapsed > current {
            match self.max_wake_ns.compare_exchange_weak(
                current,
                elapsed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }
}

/// One signaler -> many sync waiters.
///
/// This is the true blocking sync reliability/latency case. Keep the default in
/// native-thread territory; raise `RSIGNAL_STRESS_SYNC_BLOCKING_WAITERS` on a
/// host configured for very high thread counts.
#[test]
#[ignore]
fn sync_one_to_many_blocking_waiters() {
    let waiters = env_usize(
        "RSIGNAL_STRESS_SYNC_BLOCKING_WAITERS",
        DEFAULT_SYNC_BLOCKING_WAITERS,
    );
    let (tx, rx) = sync::create();
    let gate = Arc::new(Barrier::new(waiters + 1));
    let ready = Arc::new(AtomicUsize::new(0));
    let woke = Arc::new(AtomicUsize::new(0));
    let max_wake_ns = Arc::new(AtomicU64::new(0));
    let signal_started = Arc::new(OnceLock::<Instant>::new());
    let (last_woke_tx, last_woke_rx) = mpsc::channel();

    let mut handles = Vec::with_capacity(waiters);
    for _ in 0..waiters {
        let rx = rx.clone();
        let gate = gate.clone();
        let ready = ready.clone();
        let woke = woke.clone();
        let max_wake_ns = max_wake_ns.clone();
        let signal_started = signal_started.clone();
        let last_woke_tx = last_woke_tx.clone();
        handles.push(thread::spawn(move || {
            gate.wait();
            ready.fetch_add(1, Ordering::SeqCst);
            assert_eq!(rx.wait(), Ok(()));
            let woke_now = woke.fetch_add(1, Ordering::SeqCst) + 1;
            let started = signal_started
                .get()
                .expect("signal start time must be recorded before waiters wake");
            let ns = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            max_wake_ns.fetch_max(ns, Ordering::Relaxed);
            if woke_now == waiters {
                let _ = last_woke_tx.send(Duration::from_nanos(ns));
            }
        }));
    }
    drop(last_woke_tx);

    gate.wait();
    while ready.load(Ordering::SeqCst) != waiters {
        thread::yield_now();
    }
    thread::sleep(Duration::from_millis(10));
    let started = Instant::now();
    let _ = signal_started.set(started);
    assert_eq!(tx.signal(), Ok(true));
    let all_got_it = last_woke_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    for handle in handles {
        handle.join().unwrap();
    }

    let woke = woke.load(Ordering::SeqCst);
    assert_eq!(woke, waiters);
    println!(
        "sync one-to-many about_to_fire -> everyone_got_it ~= {all_got_it:?}"
    );
    assert_eq!(all_got_it, Duration::from_nanos(max_wake_ns.load(Ordering::Relaxed)));
    print_stats("sync one-to-many", waiters, 1, started, woke);
}

/// Many signalers -> one sync waiter.
#[test]
#[ignore]
fn sync_many_to_one_concurrent_signalers() {
    let signalers = env_usize(
        "RSIGNAL_STRESS_SYNC_BLOCKING_SIGNALERS",
        DEFAULT_SYNC_BLOCKING_SIGNALERS,
    );
    let (tx, rx) = sync::create();
    let signalers_vec: Vec<_> = (0..signalers).map(|_| tx.clone()).collect();
    drop(tx);

    let gate = Arc::new(Barrier::new(signalers + 1));
    let wins = Arc::new(AtomicUsize::new(0));
    let already_fired = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(signalers);
    for tx in signalers_vec {
        let gate = gate.clone();
        let wins = wins.clone();
        let already_fired = already_fired.clone();
        handles.push(thread::spawn(move || {
            gate.wait();
            match tx.signal() {
                Ok(true) => {
                    wins.fetch_add(1, Ordering::SeqCst);
                }
                Ok(false) => {
                    already_fired.fetch_add(1, Ordering::SeqCst);
                }
                Err(err) => panic!("signal unexpectedly failed: {err:?}"),
            }
        }));
    }

    let waiter = thread::spawn(move || {
        assert_eq!(rx.wait(), Ok(()));
        Instant::now()
    });

    let started = Instant::now();
    gate.wait();
    for handle in handles {
        handle.join().unwrap();
    }
    let waiter_wake = waiter.join().unwrap().saturating_duration_since(started);

    assert_eq!(wins.load(Ordering::SeqCst), 1);
    assert_eq!(
        already_fired.load(Ordering::SeqCst),
        signalers - 1,
        "every losing signaler must observe already-fired"
    );
    println!("sync many-to-one waiter wake after spawn ~= {waiter_wake:?}");
    println!(
        "sync many-to-one signal outcomes: fired={}, already_fired={}",
        wins.load(Ordering::SeqCst),
        already_fired.load(Ordering::SeqCst)
    );
    print_stats("sync many-to-one", 1, signalers, started, 1);
}

/// Many signalers -> many sync waiters.
#[test]
#[ignore]
fn sync_many_to_many_concurrent() {
    let waiters = env_usize(
        "RSIGNAL_STRESS_SYNC_BLOCKING_WAITERS",
        DEFAULT_SYNC_BLOCKING_WAITERS,
    );
    let signalers = env_usize(
        "RSIGNAL_STRESS_SYNC_BLOCKING_SIGNALERS",
        DEFAULT_SYNC_BLOCKING_SIGNALERS,
    );
    let (tx, rx) = sync::create();
    let gate = Arc::new(Barrier::new(waiters + signalers + 1));
    let wins = Arc::new(AtomicUsize::new(0));
    let already_fired = Arc::new(AtomicUsize::new(0));
    let woke = Arc::new(AtomicUsize::new(0));
    let max_wake_ns = Arc::new(AtomicU64::new(0));
    let signal_started = Arc::new(OnceLock::<Instant>::new());
    let (last_woke_tx, last_woke_rx) = mpsc::channel();

    let mut handles = Vec::with_capacity(waiters + signalers);
    for _ in 0..signalers {
        let tx = tx.clone();
        let gate = gate.clone();
        let wins = wins.clone();
        let already_fired = already_fired.clone();
        let signal_started = signal_started.clone();
        handles.push(thread::spawn(move || {
            gate.wait();
            let _ = signal_started.set(Instant::now());
            match tx.signal() {
                Ok(true) => {
                    wins.fetch_add(1, Ordering::SeqCst);
                }
                Ok(false) => {
                    already_fired.fetch_add(1, Ordering::SeqCst);
                }
                Err(err) => panic!("signal unexpectedly failed: {err:?}"),
            }
        }));
    }

    for _ in 0..waiters {
        let rx = rx.clone();
        let gate = gate.clone();
        let woke = woke.clone();
        let max_wake_ns = max_wake_ns.clone();
        let signal_started = signal_started.clone();
        let last_woke_tx = last_woke_tx.clone();
        handles.push(thread::spawn(move || {
            gate.wait();
            assert_eq!(rx.wait(), Ok(()));
            let woke_now = woke.fetch_add(1, Ordering::SeqCst) + 1;
            let started = signal_started
                .get()
                .expect("signal start time must be recorded before waiters wake");
            let ns = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            max_wake_ns.fetch_max(ns, Ordering::Relaxed);
            if woke_now == waiters {
                let _ = last_woke_tx.send(Duration::from_nanos(ns));
            }
        }));
    }
    drop(last_woke_tx);

    let started = Instant::now();
    gate.wait();
    let all_got_it = last_woke_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(wins.load(Ordering::SeqCst), 1);
    assert_eq!(
        already_fired.load(Ordering::SeqCst),
        signalers - 1,
        "every losing signaler must observe already-fired"
    );
    assert_eq!(woke.load(Ordering::SeqCst), waiters);
    println!(
        "sync many-to-many about_to_fire -> everyone_got_it ~= {all_got_it:?}"
    );
    assert_eq!(all_got_it, Duration::from_nanos(max_wake_ns.load(Ordering::Relaxed)));
    println!(
        "sync many-to-many signal outcomes: fired={}, already_fired={}",
        wins.load(Ordering::SeqCst),
        already_fired.load(Ordering::SeqCst)
    );
    print_stats(
        "sync many-to-many",
        waiters,
        signalers,
        started,
        woke.load(Ordering::SeqCst),
    );
}

/// Large sync handle-count case: one million signaler/waiter clones without
/// spawning one million native threads.
#[test]
#[ignore]
fn sync_million_handle_accounting_and_sequential_signal_reliability() {
    let waiters = env_usize("RSIGNAL_STRESS_WAITERS", DEFAULT_LARGE);
    let signalers = env_usize("RSIGNAL_STRESS_SIGNALERS", DEFAULT_LARGE);
    let started = Instant::now();
    let (tx, rx) = sync::create();
    let waiter_clones: Vec<_> = (0..waiters).map(|_| rx.clone()).collect();
    let signaler_clones: Vec<_> = (0..signalers).map(|_| tx.clone()).collect();

    assert_eq!(tx.waiter_count(), waiters + 1);
    assert_eq!(rx.signaler_count(), signalers + 1);
    assert_eq!(tx.signal(), Ok(true));
    assert_eq!(rx.wait(), Ok(()));
    let mut already_fired = 0;
    for tx in &signaler_clones {
        if tx.signal() == Ok(false) {
            already_fired += 1;
        }
    }
    assert_eq!(already_fired, signalers);
    for rx in waiter_clones.iter().take(1024) {
        assert_eq!(rx.wait(), Ok(()));
    }

    drop(waiter_clones);
    drop(signaler_clones);
    println!("sync million handles signal outcomes: fired=1, already_fired={already_fired}");
    print_stats(
        "sync million handles",
        waiters + 1,
        signalers + 1,
        started,
        waiters + 1,
    );
}

/// One signaler -> many async wait futures, all registered before the signal.
#[test]
#[ignore]
fn async_one_to_many_registered_waiters() {
    let waiters = env_usize("RSIGNAL_STRESS_WAITERS", DEFAULT_LARGE);
    let (tx, rx) = async_signal::create();
    let mut waits: Vec<_> = (0..waiters).map(|_| rx.wait()).collect();
    let wakes = Arc::new(AtomicUsize::new(0));
    let max_wake_ns = Arc::new(AtomicU64::new(0));
    let signal_started = Arc::new(OnceLock::new());
    let waker = Waker::from(Arc::new(LatencyWaker {
        wakes: wakes.clone(),
        max_wake_ns: max_wake_ns.clone(),
        start: signal_started.clone(),
    }));
    let mut cx = Context::from_waker(&waker);

    for wait in &mut waits {
        assert_eq!(Pin::new(wait).poll(&mut cx), Poll::Pending);
    }

    let started = Instant::now();
    let _ = signal_started.set(started);
    assert_eq!(tx.signal(), Ok(true));
    assert_eq!(wakes.load(Ordering::SeqCst), waiters);
    for wait in &mut waits {
        assert_eq!(Pin::new(wait).poll(&mut cx), Poll::Ready(Ok(())));
    }

    println!(
        "async one-to-many longest waker call after signal ~= {:?}",
        Duration::from_nanos(max_wake_ns.load(Ordering::Relaxed))
    );
    print_stats("async one-to-many", waiters, 1, started, waiters);
}

/// Many async signalers -> one async waiter.
#[test]
#[ignore]
fn async_many_to_one_signalers() {
    let signalers = env_usize("RSIGNAL_STRESS_SIGNALERS", DEFAULT_LARGE);
    let (tx, rx) = async_signal::create();
    let signaler_clones: Vec<_> = (0..signalers).map(|_| tx.clone()).collect();
    drop(tx);

    let started = Instant::now();
    let mut wins = 0;
    let mut already_fired = 0;
    for tx in &signaler_clones {
        match tx.signal() {
            Ok(true) => wins += 1,
            Ok(false) => already_fired += 1,
            Err(err) => panic!("signal unexpectedly failed: {err:?}"),
        }
    }
    assert_eq!(wins, 1);
    assert_eq!(already_fired, signalers - 1);
    assert_eq!(block_on(rx.wait()), Ok(()));
    println!("async many-to-one signal outcomes: fired={wins}, already_fired={already_fired}");
    print_stats("async many-to-one", 1, signalers, started, 1);
}

/// Many async signalers -> many registered async waiters.
#[test]
#[ignore]
fn async_many_to_many_registered() {
    let waiters = env_usize("RSIGNAL_STRESS_WAITERS", DEFAULT_LARGE);
    let signalers = env_usize("RSIGNAL_STRESS_SIGNALERS", DEFAULT_LARGE);
    let (tx, rx) = async_signal::create();
    let signaler_clones: Vec<_> = (0..signalers).map(|_| tx.clone()).collect();
    let mut waits: Vec<_> = (0..waiters).map(|_| rx.wait()).collect();
    let wakes = Arc::new(AtomicUsize::new(0));
    let max_wake_ns = Arc::new(AtomicU64::new(0));
    let signal_started = Arc::new(OnceLock::new());
    let waker = Waker::from(Arc::new(LatencyWaker {
        wakes: wakes.clone(),
        max_wake_ns: max_wake_ns.clone(),
        start: signal_started.clone(),
    }));
    let mut cx = Context::from_waker(&waker);

    for wait in &mut waits {
        assert_eq!(Pin::new(wait).poll(&mut cx), Poll::Pending);
    }

    let started = Instant::now();
    let _ = signal_started.set(started);
    let mut wins = 0;
    let mut already_fired = 0;
    for tx in &signaler_clones {
        match tx.signal() {
            Ok(true) => wins += 1,
            Ok(false) => already_fired += 1,
            Err(err) => panic!("signal unexpectedly failed: {err:?}"),
        }
    }
    assert_eq!(wins, 1);
    assert_eq!(already_fired, signalers - 1);
    assert_eq!(wakes.load(Ordering::SeqCst), waiters);
    for wait in &mut waits {
        assert_eq!(Pin::new(wait).poll(&mut cx), Poll::Ready(Ok(())));
    }

    println!(
        "async many-to-many longest waker call after signal ~= {:?}",
        Duration::from_nanos(max_wake_ns.load(Ordering::Relaxed))
    );
    println!("async many-to-many signal outcomes: fired={wins}, already_fired={already_fired}");
    print_stats("async many-to-many", waiters, signalers, started, waiters);
}
