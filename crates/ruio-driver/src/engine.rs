//! The driver as the executor's engine: the contract of `ruio-engine`, each
//! of whose methods is the driver's own.

use std::io;
use std::time::Duration;

use ruio_engine::{Build, Engine, LocalWaker, Unpark};

use crate::{Builder, Driver, Entered, Unparker};

impl Engine for Driver {
    type Builder = Builder;
    type Unparker = Unparker;
    type Entered = Entered;

    #[inline]
    fn id(&self) -> u64 {
        Driver::id(self)
    }

    #[inline]
    fn monotonic_now() -> Duration {
        crate::monotonic_now()
    }

    #[inline]
    fn submit(&self) -> io::Result<()> {
        Driver::submit(self)
    }

    #[inline]
    fn has_work(&self) -> bool {
        Driver::has_work(self)
    }

    #[inline]
    fn park(&self, timeout: Option<Duration>) -> io::Result<()> {
        Driver::park(self, timeout)
    }

    #[inline]
    fn wake_at(&self, deadline: Duration) -> io::Result<()> {
        Driver::wake_at(self, deadline)
    }

    #[inline]
    fn timer_fired(&self) -> bool {
        Driver::timer_fired(self)
    }

    #[inline]
    fn unparker(&self) -> Unparker {
        Driver::unparker(self)
    }

    #[inline]
    fn enter(&self) -> Entered {
        Driver::enter(self)
    }

    #[inline]
    unsafe fn set_local_waker(&self, local: LocalWaker) {
        // SAFETY: the caller keeps the contract, which is the driver's own.
        unsafe { Driver::set_local_waker(self, local) }
    }
}

impl Build for Builder {
    type Engine = Driver;

    #[inline]
    fn shutdown_timeout(self, timeout: Duration) -> Self {
        Builder::shutdown_timeout(self, timeout)
    }

    #[inline]
    fn build(&self) -> io::Result<Driver> {
        Builder::build(self)
    }
}

impl Unpark for Unparker {
    #[inline]
    fn unpark(&self) {
        Unparker::unpark(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Builder, Driver};

    #[test]
    fn the_driver_keeps_the_engine_contract() {
        ruio_engine::conformance::check::<Driver>(&Builder::new());
        ruio_engine::conformance::check::<Driver>(&Builder::new().files(0));
    }
}
