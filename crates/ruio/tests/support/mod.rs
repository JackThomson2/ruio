//! Helpers shared by the integration tests.

#![allow(dead_code)]

use std::future::Future;
use std::time::Duration;

/// How long a test waits for anything before it is considered hung.
pub(crate) const PATIENCE: Duration = Duration::from_secs(10);

/// Waits for `future`, failing the test if it takes longer than
/// [`PATIENCE`].
pub(crate) async fn within<F: Future>(future: F) -> F::Output {
    ruio::time::timeout(PATIENCE, future)
        .await
        .expect("still waiting after PATIENCE")
}
