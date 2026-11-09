//! The engine the executor runs on, chosen when the crate is built: the
//! io_uring driver of `ruio-driver` on Linux, and the park engine of
//! `ruio-engine`, which runs no I/O, elsewhere and wherever the build asks
//! for it with `--cfg ruio_engine="park"`. build.rs makes the choice, as
//! `cfg(ruio_uring)`, and Cargo.toml chooses the dependencies by the same
//! rule.
//!
//! The executor names its engine `Native` and drives it through the
//! contract of `ruio-engine` alone, calling each method by its trait,
//! `Engine::park(&engine, None)` rather than `engine.park(None)`, so that
//! no method of one engine's own is reached by accident. The engine is a
//! type, not a trait object or a parameter, so these calls compile to the
//! engine's own methods, as they would were it called directly, and the
//! choice of engine costs nothing at run time. Another engine plugs in as
//! a module beside `uring` and `park`, selected here in their place.
//!
//! Each engine's module provides:
//!
//! - `Native`, the engine;
//! - `Remote`, what a runtime's shared part keeps of the engine for other
//!   threads, whose `unpark` wakes it;
//! - `public`, if the engine has API of its own, which the crate root
//!   re-exports;
//!
//! and may give the runtime's types methods of the engine's own, as the
//! io_uring module gives a runtime its `driver`.

#[cfg(ruio_uring)]
mod uring;
#[cfg(ruio_uring)]
use uring as selected;

#[cfg(not(ruio_uring))]
mod park;
#[cfg(not(ruio_uring))]
use park as selected;

pub(crate) use ruio_engine::{Build, Engine};
pub(crate) use selected::{Native, Remote};
#[cfg(ruio_uring)]
pub(crate) use uring::public;

/// The builder of the engine.
pub(crate) type NativeBuilder = <Native as Engine>::Builder;
