# One-shot multi-signaler / multi-waiter fail-safe signaling design

## Goal

Design a signaling primitive that supports:

- one shared signal state,
- many waiters,
- many signalers,
- one-shot behavior,
- fail-safe wakeups,
- and safe duplication/cloning of the signaling pair.

The core rule is:

- a signal can be delivered at most once,
- every waiter that is waiting when the signal occurs must be released,
- every waiter that arrives after the signal must observe the already-fired state immediately,
- and every later signal attempt must be rejected.

---

## Clarified semantics

This should be treated as a single shared event with three states:

- Pending
- Fired
- Disconnected

`Fired` and `Disconnected` are both terminal and mutually exclusive: exactly one
transition out of `Pending` ever succeeds.

### State machine

1. Initially: Pending
2. The first successful `signal()` moves it to Fired
3. After Fired:
   - all current waiters are released,
   - all future waiters return immediately,
   - all future `signal()` calls are rejected and report failure
4. If the **last** signal handle is dropped while still Pending, it moves to
   Disconnected (a fail-safe: the event can never fire now):
   - all current waiters are released and told the event will never fire,
   - all future waiters return immediately with the same "disconnected" result.
   A drop after Fired changes nothing — Fired never flips to Disconnected.

### Required behavior

- `wait()` may be called before or after `signal()`.
- If `signal()` happens first, a later `wait()` must return immediately.
- If `wait()` happens first, it must block until the signal arrives.
- Once signaled, all waiters wake up.
- Once signaled, no further signal can change the state, and every later signal attempt must be told it failed because the event has already fired.
- The signaling pair may be duplicated or cloned, but all copies must share the same underlying state.

### API shape

A Rust-style shape could be:

```rust
let (tx, rx) = rsignal::sync::create();

let tx1 = tx.clone();
let rx1 = rx.clone();

// use tx and tx1 for signaling
// use rx and rx1 for waiting
```

The semantics are:

- `tx` and `tx1` are equivalent signal handles,
- `rx` and `rx1` are equivalent wait handles,
- all clones share the same underlying state,
- the first successful `signal()` wins,
- every wait handle observes the same outcome.

A more explicit API could be:

```text
Result<(), Disconnected>       wait()
Result<bool, NoReceivers>      signal()
bool                           is_signaled() const
usize                          signaler_count() const
usize                          waiter_count() const
```

Where:

- `wait()` blocks until the event reaches a terminal state, then returns `Ok(())`
  if it fired, or `Err(Disconnected)` if every signaler was dropped first,
- `signal()` returns `Ok(true)` only for the first successful signal,
- `signal()` returns `Ok(false)` for every later attempt, meaning this event was already fired and this caller can never fire it,
- `signal()` returns `Err(NoReceivers)` when every waiter has been dropped, so no
  one could ever observe the signal; in that case the event is left unfired
  (mirroring `std::sync::mpsc::Sender::send`, which fails once the receiver is gone),
- `is_signaled()` reports the current state,
- `signaler_count()` / `waiter_count()` report a snapshot of how many live
  handles of each kind share the event; both are available on either handle so a
  signaler can see if anyone is waiting and a waiter can see if anyone can signal.

`signal()`'s already-fired check takes precedence over `NoReceivers`: if the event
fired earlier (necessarily while a receiver existed), a later call reports
`Ok(false)` even if all receivers have since been dropped.

Because `wait()` now has two terminal outcomes (fired vs. disconnected), it
returns a `Result` rather than nothing. This mirrors `std::sync::mpsc::Receiver`,
where `recv()` yields the value or a disconnect error. A plain boolean is avoided
because the timed variant below would then have two different `false` meanings.

Optional variants can be added, such as:

```text
Result<(), WaitTimeoutError>   wait_timeout(duration)   // Timeout | Disconnected
Result<(), TryWaitError>       try_wait()               // Pending | Disconnected
```

Each keeps `Ok(())` for "fired" and uses a distinct error variant per non-fired
outcome, so timeout, disconnect, and still-pending are never conflated.

### Lock-free feasibility

The state transition itself can be made lock-free with an atomic compare-and-swap or equivalent. The challenge is the waiting part.

In practice:

- `signal()` can be implemented with atomics and is effectively lock-free / wait-free for the state change,
- `wait()` generally needs some form of suspension mechanism, such as a condition variable, futex, or park/unpark,
- a busy-spin version is possible but not efficient and wastes CPU,
- so the best practical design is atomic state plus a blocking primitive rather than a fully lock-free wait path.

So the answer is:

- yes, the one-shot state change can be lock-free,
- but a fully efficient and general lock-free wait/signal pair is usually not practical without trading off latency, CPU usage, or portability.

---

## Design principles

### 1. Use a shared state object

The wait handle and signal handle should not each own independent state. They should both reference the same shared object.

That shared object should contain:

- a boolean or atomic flag for fired/not fired,
- a mutex,
- a condition variable (or equivalent wake mechanism).

### 2. Prefer atomic state for the one-shot transition

The transition from Pending to Fired must be atomic.

The best primitive for this is:

- an atomic flag or compare-and-swap operation,
- followed by a wake-up operation.

This ensures that only the first signal wins.

If atomics are used directly, the successful signal should publish the fired state with release ordering, and waiters/readers should observe it with acquire ordering. If the implementation keeps the fired state protected entirely by the mutex, the mutex can provide the required synchronization instead.

### 3. Use a condition variable for waiting

Waiters should block efficiently until the signal is delivered.

The condition variable should be used together with the mutex and a predicate loop so that:

- no waiter misses the event,
- no spurious wakeups cause incorrect behavior,
- and state changes remain consistent.

### 4. Make the signal path fail-safe

The implementation should ensure that a signal cannot be lost even if a waiter starts after the signal.

That means the wait path must check the fired flag before blocking, and the signal path must set the flag before waking waiters. If the design uses an atomic fast path, the signal path should still coordinate with the same mutex used by the condition variable before calling `notify_all`.

---

## Recommended primitive set

For languages with standard concurrency support, the ideal combination is:

- atomic boolean / atomic flag,
- mutex,
- condition variable.

This is the closest general-purpose equivalent to a one-shot event with broadcast wakeups.

### Why these primitives

- Atomic flag: ensures exactly one successful transition to Fired
- Mutex: protects shared state safely
- Condition variable: allows many waiters to sleep until the event is fired

This is typically better than using a plain boolean alone because it avoids races and avoids busy-waiting. A plain boolean can still be correct if every access to it is protected by the mutex, but then the one-shot "first signal wins" transition is mutex-based rather than atomic.

---

## Reference implementation sketch

### Language-agnostic behavior

```text
shared state:
  fired = false  // atomic, or protected by mutex in a mutex-only design
  mutex
  condition_variable

wait():
  // Fast path: no mutex needed. The acquire load synchronizes-with the
  // release store in signal()'s CAS, so once fired is observed true there
  // is nothing left to wait for and all prior writes are already visible.
  if atomic_load_acquire(fired) == true:
    return immediately

  lock mutex
  while atomic_load_acquire(fired) == false:
    wait on condition_variable  // atomically unlocks mutex, sleeps, then reacquires mutex
  unlock mutex
  return

signal():
  // success ordering = release (publish), failure ordering = relaxed
  // (a losing signaler reads no shared state and only returns false).
  if atomic_compare_exchange(fired, false, true, release, relaxed) succeeds:
    lock mutex
    notify_all waiters   // done under the mutex here for simplicity; notifying
    unlock mutex         // just after unlock is also valid, since the mutex
    return true          // acquisition is the real synchronization point

  // The event has already fired. This attempt did not signal anything
  // and can never become successful later.
  return false
```

### Important detail

The notify should happen only after the fired state is committed, so that waiters cannot miss the event.

The waiter must check the fired predicate in a loop while holding the mutex because condition variables may wake spuriously. The signaler should take the same mutex before notifying, even if the state transition itself was done with an atomic compare-and-swap. This coordinates with waiters that have checked the state and are about to sleep.

A few ordering details worth stating explicitly:

- The lock-free fast path in `wait()` may skip the mutex entirely. Its acquire load synchronizes-with the release store performed by the successful `signal()` CAS, so once a waiter observes `fired == true` there is nothing left to block on and every write published before the signal is already visible. The mutex is only needed on the slow path, where a waiter must be able to sleep and be woken.
- Only the success ordering of the compare-and-swap needs `release`, to publish the transition. The failure ordering can be `relaxed`: a losing signaler reads no shared state and simply returns `false`.
- Whether `notify_all` is called while still holding the mutex or immediately after releasing it does not affect correctness. The synchronization that prevents a lost wakeup is the signaler acquiring the mutex at all after setting `fired`, which guarantees any waiter that read `fired == false` has already parked on the condition variable. Notifying under the lock is simplest; notifying just after unlock can reduce the chance a woken waiter immediately blocks on the still-held mutex.

For a mutex-only implementation, replace the atomic loads and compare-exchange with reads and writes performed while holding the mutex.

---

## Duplication and cloning

Each clone should point to the same shared state object.

That means:

- copying the handle must not create a second independent event,
- all handles must share the same lifecycle,
- and the state must remain valid as long as at least one handle exists.

A common implementation strategy is:

- shared ownership of the state object,
- handles are lightweight references to that shared state.

---

## Failure modes to avoid

### 1. Lost wakeup

A waiter must not miss the event if the signal arrives before it starts waiting.

This is solved by checking the fired flag before waiting and by making the signal set the flag before notifying.

### 2. Multiple successful signals

Only the first `signal()` should succeed.

This is solved with an atomic compare-and-swap or equivalent once-only transition. Every caller after the winner must receive an explicit failure result so it can distinguish "I fired the event" from "someone else already fired it."

### 3. Deadlock or starvation

The implementation must not block forever if the signal already happened.

This is solved by making `wait()` return immediately when the shared state is already fired.

### 4. Broken sharing between clones

Copies must not behave like separate events.

This is solved by sharing one internal state object across all duplicates.

### 5. Waiter blocked forever after all signalers vanish

If every signal handle is dropped without firing, a blocked waiter would
otherwise wait forever for a signal that can never come.

This is solved by counting live signal handles and, when the last one is dropped
while still Pending, transitioning to Disconnected and waking all waiters with an
explicit "disconnected" result. The count is decremented in the signal handle's
destructor; the transition uses the same once-only compare-and-swap and the same
mutex-before-notify handshake as `signal()`, so it cannot race with a real fire.

### 6. Signaling into the void

Symmetrically, if every waiter is dropped, a signal can reach no one.

This is solved by counting live wait handles. Because a wait handle can only be
created by cloning an existing one, the count is monotonic-to-zero: once it hits
zero it stays there, so `signal()` can reliably return `NoReceivers` and skip
firing. No wakeup is needed on the last waiter drop, since signalers never block.
Both counts are also exposed as `signaler_count()` / `waiter_count()` for
introspection (best-effort snapshots).

---

## Test checklist

The implementation should include tests for:

- `wait()` before `signal()` blocks, then returns after the signal.
- `signal()` before `wait()` makes later waits return immediately.
- many waiters are all released by one successful signal.
- many concurrent signalers produce exactly one `true` result and all other attempts return `false`.
- repeated `signal()` calls after Fired always return `false`.
- cloned signal handles share the same fired state.
- cloned wait handles observe the same fired state.
- repeated waits after Fired return immediately.
- spurious condition-variable wakeups cannot make `wait()` return before a terminal state.
- dropping one handle does not invalidate the shared state while other handles still exist.
- dropping the last signal handle while Pending releases all waiters with a disconnected result.
- a blocked waiter is woken when the last signal handle is dropped.
- dropping a signal handle after Fired does not change the fired result.
- interleaved fire-vs-drop races leave every waiter with the same single terminal outcome.
- `signal()` returns `NoReceivers` (and does not fire) once every wait handle is dropped.
- `signal()` still succeeds while at least one wait handle survives.
- an already-fired event reports `Ok(false)`, not `NoReceivers`, after its receivers drop.
- `signaler_count()` / `waiter_count()` track clones and drops from both handle types.

---

## Suggested terminology

To keep the design clear, the following names are recommended:

- `SignalHandle`
- `WaitHandle`
- `OneShotEvent`
- `SharedSignal`

The pair can be modeled as:

```text
SharedSignal signal = make_shared_signal()
SignalHandle a = signal
SignalHandle b = signal
WaitHandle c = signal
WaitHandle d = signal
```

---

## Async variant

The same state machine has a runtime-agnostic async form. Only the wait side
changes; `signal()` stays non-blocking and identical.

- `wait()` returns a future resolving to `Ok(())` on fire or `Err(Disconnected)`
  when the last signaler drops.
- The mutex + condvar are replaced by a mutex-guarded map of parked tasks' wakers.
  A terminal transition drains the map and wakes every waker *after* releasing the
  lock (so a synchronous re-poll cannot deadlock on re-entry).
- The future re-checks the terminal state while holding the registry lock before
  parking — the async analog of checking the predicate under the condvar mutex —
  which prevents lost wakeups.
- The future deregisters its waker on drop, so cancelling a pending await leaves no
  stale entry.
- There is no async `wait_timeout`: timeouts are a runtime concern, so callers wrap
  `wait()` with their executor's timeout combinator.

## Final clarification

The intended system is a shared one-shot broadcast event:

- many waiters can wait,
- many signalers can attempt to fire it,
- only the first successful signal wins,
- once fired, all waiters are released,
- and all future signals are rejected with an explicit failure result.

This is best expressed as a shared state object with an atomic one-shot flag plus a condition variable and mutex.
