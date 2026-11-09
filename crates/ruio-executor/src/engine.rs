//! The engine the executor runs on: on Linux, the io_uring driver of
//! `ruio-driver`.
//!
//! The executor names its engine `Native` and drives it through the
//! contract of `ruio-engine` alone, calling each method by its trait,
//! `Engine::park(&engine, None)` rather than `engine.park(None)`, so that
//! no method of one engine's own is reached by accident. The engine is a
//! type, not a trait object or a parameter, so these calls compile to the
//! engine's own methods, as they would were it called directly, and the
//! choice of engine costs nothing at run time. Another engine plugs in as
//! a module beside `uring`, selected here in its place.
//!
//! Each engine's module provides:
//!
//! - `Native`, the engine;
//! - `Remote`, what a runtime's shared part keeps of the engine for other
//!   threads, whose `unpark` wakes it;
//! - `public`, the engine's own API that the crate root re-exports;
//!
//! and may give the runtime's types methods of the engine's own, as the
//! io_uring module gives a runtime its `driver`.

mod uring;

pub(crate) use ruio_engine::{Build, Engine};
pub(crate) use uring::{Native, Remote, public};

/// The builder of the engine.
pub(crate) type NativeBuilder = <Native as Engine>::Builder;
