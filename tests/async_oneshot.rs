//! Behavioral tests for the async (`future`) one-shot broadcast event.
//!
//! To stay runtime-agnostic and dependency-free, tests use a tiny thread-parking
//! `block_on`, plus a counting waker for the manual-poll (waker-wiring) tests.

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use rsignals::r#async::{create, Disconnected, NoReceivers, TryWaitError};

const MANY: usize = 12;

/// A waker that unparks the thread that is blocking on the future.
struct ThreadWaker(thread::Thread);
impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Minimal single-future executor: poll, park until woken, repeat.
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

/// A waker that counts how many times it is invoked.
struct CountWaker(Arc<AtomicUsize>);
impl Wake for CountWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_waker() -> (Waker, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    (Waker::from(Arc::new(CountWaker(count.clone()))), count)
}

/// 1. `.await` before the signal blocks, then completes once fired.
#[test]
fn await_completes_after_later_signal() {
    let (tx, rx) = create();
    let (send, recv) = mpsc::channel();
    thread::spawn(move || {
        let _ = send.send(block_on(rx.wait()));
    });

    // The awaiter should still be parked here.
    assert!(recv.recv_timeout(Duration::from_millis(80)).is_err());

    assert_eq!(tx.signal(), Ok(true));
    assert_eq!(
        recv.recv_timeout(Duration::from_secs(5)).unwrap(),
        Ok(()),
        "awaiter must complete after the signal"
    );
}

/// 2. A signal before the `.await` completes it immediately.
#[test]
fn await_after_signal_is_immediate() {
    let (tx, rx) = create();
    assert_eq!(tx.signal(), Ok(true));
    assert_eq!(block_on(rx.wait()), Ok(()));
}

/// 3. Many awaiters are all released by one signal.
#[test]
fn many_awaiters_released_by_one_signal() {
    let (tx, rx) = create();
    let barrier = Arc::new(Barrier::new(MANY + 1));
    let (send, recv) = mpsc::channel();

    for _ in 0..MANY {
        let rx = rx.clone();
        let barrier = barrier.clone();
        let send = send.clone();
        thread::spawn(move || {
            barrier.wait();
            let _ = send.send(block_on(rx.wait()));
        });
    }
    drop(send);

    barrier.wait();
    thread::sleep(Duration::from_millis(30));
    assert_eq!(tx.signal(), Ok(true));

    for _ in 0..MANY {
        assert_eq!(recv.recv_timeout(Duration::from_secs(5)).unwrap(), Ok(()));
    }
}

/// 4. Dropping all signalers completes an awaiter with `Disconnected`.
#[test]
fn await_disconnects_when_all_signalers_dropped() {
    let (tx, rx) = create();
    let (send, recv) = mpsc::channel();
    thread::spawn(move || {
        let _ = send.send(block_on(rx.wait()));
    });

    thread::sleep(Duration::from_millis(80));
    drop(tx);

    assert_eq!(
        recv.recv_timeout(Duration::from_secs(5)).unwrap(),
        Err(Disconnected),
        "awaiter must be released with Disconnected"
    );
}

/// 5. `signal()` reports `NoReceivers` once every waiter is dropped.
#[test]
fn signal_reports_no_receivers() {
    let (tx, rx) = create();
    drop(rx);
    assert_eq!(tx.signal(), Err(NoReceivers));
    assert!(!tx.is_signaled());
}

/// 6. Non-blocking snapshots track the state.
#[test]
fn snapshots_track_state() {
    let (tx, rx) = create();
    assert_eq!(rx.try_wait(), Err(TryWaitError::Pending));
    assert!(!rx.is_signaled());
    assert!(!rx.is_disconnected());

    assert_eq!(tx.signal(), Ok(true));
    assert_eq!(rx.try_wait(), Ok(()));
    assert!(rx.is_signaled());
}

/// 7. try_wait reports Disconnected after all signalers drop.
#[test]
fn try_wait_reports_disconnected() {
    let (tx, rx) = create();
    drop(tx);
    assert!(rx.is_disconnected());
    assert_eq!(rx.try_wait(), Err(TryWaitError::Disconnected));
}

/// 8. Sibling counts track clones and drops from both handle types.
#[test]
fn counts_track_clones() {
    let (tx, rx) = create();
    assert_eq!(tx.signaler_count(), 1);
    assert_eq!(tx.waiter_count(), 1);

    let tx2 = tx.clone();
    let rx2 = rx.clone();
    assert_eq!(rx.signaler_count(), 2);
    assert_eq!(tx.waiter_count(), 2);

    drop(tx2);
    drop(rx2);
    assert_eq!(rx.signaler_count(), 1);
    assert_eq!(tx.waiter_count(), 1);
}

/// 9. Manual poll: the future registers its waker and the signal wakes it.
#[test]
fn poll_registers_waker_and_signal_wakes_it() {
    let (tx, rx) = create();
    let (waker, count) = counting_waker();
    let mut cx = Context::from_waker(&waker);

    let mut fut = pin!(rx.wait());
    assert_eq!(fut.as_mut().poll(&mut cx), Poll::Pending);
    assert_eq!(count.load(Ordering::SeqCst), 0, "no wake before signal");

    assert_eq!(tx.signal(), Ok(true));
    assert!(
        count.load(Ordering::SeqCst) >= 1,
        "signal must wake the registered waker"
    );
    assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
}

/// 10. Manual poll: dropping all signalers wakes the registered waker.
#[test]
fn poll_registers_waker_and_disconnect_wakes_it() {
    let (tx, rx) = create();
    let (waker, count) = counting_waker();
    let mut cx = Context::from_waker(&waker);

    let mut fut = pin!(rx.wait());
    assert_eq!(fut.as_mut().poll(&mut cx), Poll::Pending);

    drop(tx);
    assert!(
        count.load(Ordering::SeqCst) >= 1,
        "disconnect must wake the registered waker"
    );
    assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(Err(Disconnected)));
}

/// 11. Cancelling (dropping) a polled-but-incomplete future must not break other
///     waiters, and must not leave a stale waker that mis-fires.
#[test]
fn cancelling_a_pending_future_is_safe() {
    let (tx, rx) = create();

    // Poll a future to register it, then drop it (cancellation).
    {
        let (waker, _count) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = pin!(rx.wait());
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Pending);
    } // fut dropped here; its registration must be cleaned up

    // A real awaiter still works end-to-end.
    let rx2 = rx.clone();
    let h = thread::spawn(move || block_on(rx2.wait()));
    thread::sleep(Duration::from_millis(30));
    assert_eq!(tx.signal(), Ok(true));
    assert_eq!(h.join().unwrap(), Ok(()));
}

/// 12. The same waiter can await repeatedly after the fire; each is immediate.
#[test]
fn repeated_await_after_fire() {
    let (tx, rx) = create();
    assert_eq!(tx.signal(), Ok(true));
    for _ in 0..MANY {
        assert_eq!(block_on(rx.wait()), Ok(()));
    }
}
