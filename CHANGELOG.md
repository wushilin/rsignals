# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-05

Initial release.

### Added

- One-shot, multi-signaler / multi-waiter (MPMC) broadcast event with a
  three-state model: `Pending -> {Fired, Disconnected}`, terminal states
  mutually exclusive.
- `rsignals::sync` — blocking variant (`wait`, `wait_timeout`, `try_wait`)
  backed by a mutex + condvar with a lock-free `Acquire` fast path.
- `rsignals::r#async` — runtime-agnostic async variant (`wait().await`,
  `try_wait`) backed by a `Waker` registry; cancelled futures deregister
  their waker on drop.
- Fail-safe semantics on both sides: waiters get `Disconnected` when every
  signaler drops without firing; `signal()` returns `NoReceivers` when every
  waiter is gone.
- mpsc-style error types: `Disconnected`, `NoReceivers`, `WaitTimeoutError`,
  `TryWaitError`.
- Live-handle snapshots: `signaler_count()` / `waiter_count()`.
- Zero dependencies, `#![forbid(unsafe_code)]`.

[Unreleased]: https://github.com/wushilin/rsignal/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/wushilin/rsignal/releases/tag/v0.1.0
