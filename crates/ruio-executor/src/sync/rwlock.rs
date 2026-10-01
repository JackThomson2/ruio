//! A reader-writer lock whose guards may be held across awaits.

use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use super::{Semaphore, SemaphorePermit};

/// How many readers a lock admits at once: a writer takes them all.
const MAX_READERS: usize = u32::MAX as usize >> 3;

/// A lock giving any number of tasks at a time shared access to a value,
/// or one task exclusive access, which they may hold across awaits.
///
/// Tasks get the lock in the order they asked for it: a writer waiting
/// for the readers to finish holds up the readers that ask after it, so
/// that a stream of readers cannot keep it waiting for ever. It is built
/// on a [`Semaphore`], which a reader takes one permit of and a writer
/// takes all of, and works across threads.
///
/// Readers on several threads share the value, so the lock is `Sync` only
/// if the value is:
///
/// ```compile_fail
/// fn sync<T: Sync>() {}
/// sync::<ruio_executor::sync::RwLock<std::cell::Cell<u8>>>();
/// ```
///
/// ```
/// use ruio_executor::sync::RwLock;
///
/// # ruio_executor::Runtime::new()?.block_on(async {
/// let lock = RwLock::new(5);
/// {
///     let first = lock.read().await;
///     let second = lock.read().await;
///     assert_eq!(*first + *second, 10);
/// }
/// *lock.write().await += 1;
/// assert_eq!(*lock.read().await, 6);
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct RwLock<T: ?Sized> {
    semaphore: Semaphore,
    value: UnsafeCell<T>,
}

// SAFETY: the value moves with the lock.
unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
// SAFETY: readers on several threads share the value, which must so be
// `Sync`, and a writer on any thread has it as `&mut T`, which must so be
// `Send`.
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    /// A lock holding `value`, unlocked.
    pub const fn new(value: T) -> Self {
        Self {
            semaphore: Semaphore::new(MAX_READERS),
            value: UnsafeCell::new(value),
        }
    }

    /// The value, which nothing can be using.
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Waits for shared access, after the tasks that asked before.
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        let permit = self.semaphore.acquire().await;
        RwLockReadGuard::new(self, permit)
    }

    /// Shared access, if no writer holds the lock or waits for it.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        let permit = self.semaphore.try_acquire()?;
        Some(RwLockReadGuard::new(self, permit))
    }

    /// Waits for exclusive access, after the tasks that asked before.
    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        let permit = self.semaphore.acquire_many(MAX_READERS).await;
        RwLockWriteGuard::new(self, permit)
    }

    /// Exclusive access, if nobody holds the lock or waits for it.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        let permit = self.semaphore.try_acquire_many(MAX_READERS)?;
        Some(RwLockWriteGuard::new(self, permit))
    }

    /// The value, which the `&mut` borrow keeps anyone else from locking.
    pub fn get_mut(&mut self) -> &mut T {
        self.value.get_mut()
    }
}

impl<T: Default> Default for RwLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for RwLock<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("RwLock");
        match self.try_read() {
            Some(guard) => debug.field("value", &&*guard),
            None => debug.field("value", &format_args!("<locked>")),
        };
        debug.finish_non_exhaustive()
    }
}

/// Shared access to the value of an [`RwLock`], which goes when dropped.
#[must_use = "dropping a guard lets go of the lock at once"]
pub struct RwLockReadGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    /// A reader's permit, which goes back as the guard is dropped.
    _permit: SemaphorePermit<'a>,
    _value: PhantomData<&'a T>,
}

impl<'a, T: ?Sized> RwLockReadGuard<'a, T> {
    fn new(lock: &'a RwLock<T>, permit: SemaphorePermit<'a>) -> Self {
        Self {
            lock,
            _permit: permit,
            _value: PhantomData,
        }
    }
}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard holds a reader's permit, so no writer, which
        // needs every permit, reaches the value while this borrow lasts.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

/// Exclusive access to the value of an [`RwLock`], which goes when
/// dropped.
#[must_use = "dropping a guard lets go of the lock at once"]
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    /// Every permit, which go back as the guard is dropped.
    _permit: SemaphorePermit<'a>,
    _value: PhantomData<&'a mut T>,
}

impl<'a, T: ?Sized> RwLockWriteGuard<'a, T> {
    fn new(lock: &'a RwLock<T>, permit: SemaphorePermit<'a>) -> Self {
        Self {
            lock,
            _permit: permit,
            _value: PhantomData,
        }
    }
}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard holds every permit, so no other guard reaches
        // the value while this borrow lasts.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as for `deref`, and the guard is borrowed mutably.
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}
