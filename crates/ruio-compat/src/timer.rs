//! hyper's timer, on ruio's timers.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use hyper::rt;
use ruio::time;

/// A [`hyper::rt::Timer`] whose sleeps are ruio's: what hyper's timeouts
/// need. Given one, hyper's HTTP/1.1 server cuts off a client that takes
/// longer than its `header_read_timeout`, 30 seconds unless set otherwise,
/// to send a request's head; without one, it waits for ever.
///
/// ```
/// use hyper::server::conn::http1;
/// use ruio_compat::Timer;
///
/// let mut http = http1::Builder::new();
/// http.timer(Timer::new());
/// ```
///
/// A sleep's timer is in the wheel of the runtime that polls it, which for
/// hyper is that of the task serving the connection. ruio's sleeps are
/// `Send` and `Sync`, as hyper asks of a timer's, and the timer resets one
/// by moving its deadline, without making a new one.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct Timer;

impl Timer {
    /// A timer.
    pub fn new() -> Self {
        Self
    }
}

impl rt::Timer for Timer {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn rt::Sleep>> {
        Box::pin(Sleep(time::sleep(duration)))
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn rt::Sleep>> {
        Box::pin(Sleep(time::sleep_until(deadline)))
    }

    fn reset(&self, sleep: &mut Pin<Box<dyn rt::Sleep>>, new_deadline: Instant) {
        match sleep.as_mut().downcast_mut_pin::<Sleep>() {
            Some(ours) => ours.get_mut().0.reset(new_deadline),
            None => *sleep = self.sleep_until(new_deadline),
        }
    }
}

/// A sleep of ruio's, as hyper takes it.
struct Sleep(time::Sleep);

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

impl rt::Sleep for Sleep {}
