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
    use std::thread;
    use std::time::{Duration, Instant};

    use ruio_engine::{Build, Engine, Unpark};

    use crate::Builder;

    /// Turns any engine as an executor does, through the contract alone.
    fn exercise<E: Engine>(builder: &E::Builder) {
        let engine = builder.build().unwrap();
        engine.submit().unwrap();

        // An unpark before the park makes it return at once, and once.
        let unparker = engine.unparker();
        unparker.unpark();
        unparker.unpark();
        assert!(engine.has_work());
        engine.park(None).unwrap();
        let started = Instant::now();
        engine.park(Some(Duration::from_millis(20))).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(20));

        // An unpark from another thread ends a park.
        let remote = engine.unparker();
        let waker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            remote.unpark();
        });
        engine.park(None).unwrap();
        waker.join().unwrap();

        // The timer ends a park by its deadline, and says that it fired.
        assert!(!engine.timer_fired());
        let deadline = E::monotonic_now() + Duration::from_millis(20);
        engine.wake_at(deadline).unwrap();
        while !engine.timer_fired() {
            engine.park(None).unwrap();
        }
        assert!(E::monotonic_now() >= deadline);
        assert!(!engine.timer_fired(), "asking clears it");
    }

    #[test]
    fn the_driver_parks_unparks_and_fires_its_timer_as_an_engine() {
        exercise::<crate::Driver>(&Builder::new().shutdown_timeout(Duration::from_secs(1)));
    }
}
