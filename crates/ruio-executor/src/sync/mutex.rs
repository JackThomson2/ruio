//! A mutual exclusion lock whose guard may be held across awaits.

use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use super::{Semaphore, SemaphorePermit};

/// A lock giving one task at a time access to a value, which it may hold
/// across awaits; tasks wait for it in the order they asked.
///
/// It is built on a [`Semaphore`] of one permit, and so is fair and works
/// across threads. A lock held only between awaits is cheaper as a
/// `RefCell`, on one thread, or a `std::sync::Mutex`.
///
/// ```
/// use std::sync::Arc;
///
/// use ruio_executor::sync::Mutex;
///
/// # ruio_executor::Runtime::new()?.block_on(async {
/// let log = Arc::new(Mutex::new(Vec::new()));
/// let writer = {
///     let log = Arc::clone(&log);
///     ruio_executor::spawn(async move {
///         let mut log = log.lock().await;
///         log.push("first");
///         ruio_executor::yield_now().await;
///         log.push("second");
///     })
/// };
/// writer.await.unwrap();
/// assert_eq!(*log.lock().await, ["first", "second"]);
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Mutex<T: ?Sized> {
    semaphore: Semaphore,
    value: UnsafeCell<T>,
}

// SAFETY: the value moves with the mutex.
unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}
// SAFETY: one guard at a time reaches the value, as `&mut T`, which only
// needs `T: Send` to be handed between threads.
unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

impl<T> Mutex<T> {
    /// A mutex holding `value`, unlocked.
    pub const fn new(value: T) -> Self {
        Self {
            semaphore: Semaphore::new(1),
            value: UnsafeCell::new(value),
        }
    }

    /// The value, which nothing can be using.
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Waits for the lock, after the tasks that asked before.
    pub async fn lock(&self) -> MutexGuard<'_, T> {
        let permit = self.semaphore.acquire().await;
        MutexGuard::new(self, permit)
    }

    /// The lock, if nobody holds it or waits for it.
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        let permit = self.semaphore.try_acquire()?;
        Some(MutexGuard::new(self, permit))
    }

    /// The value, which the `&mut` borrow keeps anyone else from locking.
    pub fn get_mut(&mut self) -> &mut T {
        self.value.get_mut()
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for Mutex<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Mutex");
        match self.try_lock() {
            Some(guard) => debug.field("value", &&*guard),
            None => debug.field("value", &format_args!("<locked>")),
        };
        debug.finish_non_exhaustive()
    }
}

/// Access to the value of a locked [`Mutex`], which unlocks it when
/// dropped.
///
/// Shared, a guard shares the value, so it is `Sync` only if the value is:
///
/// ```compile_fail
/// fn sync<T: Sync>() {}
/// sync::<ruio_executor::sync::MutexGuard<'static, std::cell::Cell<u8>>>();
/// ```
///
/// And a mutex is no more `Send` than its value:
///
/// ```compile_fail
/// fn send<T: Send>() {}
/// send::<ruio_executor::sync::Mutex<std::rc::Rc<u8>>>();
/// ```
#[must_use = "dropping a guard unlocks the mutex at once"]
pub struct MutexGuard<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
    /// The mutex's one permit, which goes back as the guard is dropped.
    _permit: SemaphorePermit<'a>,
    /// Shared, the guard shares the value, which must then be `Sync`.
    _value: PhantomData<&'a mut T>,
}

impl<'a, T: ?Sized> MutexGuard<'a, T> {
    fn new(mutex: &'a Mutex<T>, permit: SemaphorePermit<'a>) -> Self {
        Self {
            mutex,
            _permit: permit,
            _value: PhantomData,
        }
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard holds the mutex's one permit, so no other guard
        // reaches the value while this borrow lasts.
        unsafe { &*self.mutex.value.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as for `deref`, and the guard is borrowed mutably.
        unsafe { &mut *self.mutex.value.get() }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}
