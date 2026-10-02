//! The payload of an operation nobody waits for, kept in its table slot
//! until the kernel is done with it.

use std::fmt;
use std::mem::MaybeUninit;
use std::ptr;

use crate::op::{Completion, Operation, Orphan};

/// How many bytes of an operation a slot holds in place. A write behind's
/// send with a bundle of leased buffers, the largest payload the runtime
/// detaches on a hot path, takes 136.
const INLINE: usize = 160;

/// The payload of a request whose future was dropped, or that was detached:
/// the memory the kernel may still use, and what to do with its completion.
///
/// A payload that fits lies in the table's slot itself, so that detaching
/// an operation, as every write behind does, allocates nothing; one that
/// does not fit, or needs stricter alignment, is boxed, and the box lies
/// in the slot instead. Either way the slot holds the bytes and a table of
/// what to do with them, so the driver treats every payload alike.
pub struct Payload {
    storage: Storage,
    vtable: &'static VTable,
}

/// Where a payload's bytes lie: room for [`INLINE`] of them, aligned as a
/// pointer is.
#[repr(C, align(8))]
struct Storage(MaybeUninit<[u8; INLINE]>);

/// What the driver can do with the bytes of a payload of type `P`, which
/// lie at the pointer each function is given.
struct VTable {
    /// Hands the payload a completion; see [`Orphan::deliver`].
    deliver: unsafe fn(*mut u8, Completion),
    /// Drops the payload in place.
    drop: unsafe fn(*mut u8),
}

/// The vtable of a payload of type `P`.
struct Shape<P>(P);

impl<P: Orphan> Shape<P> {
    const VTABLE: &'static VTable = &VTable {
        deliver: deliver::<P>,
        drop: drop::<P>,
    };

    /// Whether a `P` fits in a slot's storage.
    const FITS: bool = size_of::<P>() <= INLINE && align_of::<P>() <= align_of::<Storage>();
}

/// Hands the `P` at `payload` a completion.
///
/// # Safety
///
/// `payload` must point to an initialized `P`, which only this function
/// uses meanwhile.
unsafe fn deliver<P: Orphan>(payload: *mut u8, completion: Completion) {
    // SAFETY: the caller promises an initialized `P` that nothing else uses.
    let payload = unsafe { &mut *payload.cast::<P>() };
    payload.deliver(completion);
}

/// Drops the `P` at `payload` in place.
///
/// # Safety
///
/// `payload` must point to an initialized `P`, which nothing uses again.
unsafe fn drop<P>(payload: *mut u8) {
    // SAFETY: the caller promises an initialized `P` that nothing uses again.
    unsafe { ptr::drop_in_place(payload.cast::<P>()) };
}

impl Payload {
    /// The payload of a single-shot operation, which completes once; the
    /// operation goes in place if it fits, and in a box otherwise.
    pub(crate) fn operation<T: Operation>(operation: T) -> Self {
        if Shape::<Option<T>>::FITS {
            Self::inline(Some(operation))
        } else {
            Self::boxed(Some(operation))
        }
    }

    /// A payload kept in a box, whichever its size: the box goes in place.
    pub(crate) fn boxed<P: Orphan + 'static>(payload: P) -> Self {
        let payload: Box<dyn Orphan> = Box::new(payload);
        Self::inline(payload)
    }

    /// A payload written into the storage, which `P` must fit.
    fn inline<P: Orphan>(payload: P) -> Self {
        debug_assert!(Shape::<P>::FITS, "the payload does not fit in a slot");
        let mut storage = Storage(MaybeUninit::uninit());
        // SAFETY: the storage has room for a `P`, aligned for one, as the
        // callers check; it is uninitialized, so nothing is overwritten.
        unsafe { storage.0.as_mut_ptr().cast::<P>().write(payload) };
        Self {
            storage,
            vtable: Shape::<P>::VTABLE,
        }
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.storage.0.as_mut_ptr().cast()
    }
}

impl Orphan for Payload {
    fn deliver(&mut self, completion: Completion) {
        let (deliver, payload) = (self.vtable.deliver, self.as_mut_ptr());
        // SAFETY: the storage holds an initialized payload of the vtable's
        // type from construction until it is dropped, and this borrows it
        // exclusively.
        unsafe { deliver(payload, completion) };
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Payload").finish_non_exhaustive()
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        let (drop, payload) = (self.vtable.drop, self.as_mut_ptr());
        // SAFETY: the storage holds an initialized payload of the vtable's
        // type, which nothing uses after this.
        unsafe { drop(payload) };
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use ruio_sys as sys;

    use super::*;

    /// What an operation of `N` bytes reports.
    #[derive(Default)]
    struct Report {
        completed: Cell<Option<i32>>,
        dropped: Cell<bool>,
    }

    /// An operation holding `N` bytes, which notes its completion and its
    /// drop.
    struct Bytes<const N: usize> {
        bytes: [u8; N],
        report: Rc<Report>,
    }

    impl<const N: usize> Bytes<N> {
        fn new() -> (Self, Rc<Report>) {
            let report = Rc::new(Report::default());
            let sized = Self {
                bytes: [N as u8; N],
                report: report.clone(),
            };
            (sized, report)
        }
    }

    // SAFETY: the entry is never filled in.
    unsafe impl<const N: usize> Operation for Bytes<N> {
        type Output = ();

        fn prepare(&mut self, _: &mut sys::io_uring_sqe) {}

        fn complete(self, completion: Completion) {
            assert!(self.bytes.iter().all(|&byte| byte == N as u8));
            self.report.completed.set(Some(completion.res()));
        }
    }

    impl<const N: usize> Drop for Bytes<N> {
        fn drop(&mut self) {
            self.report.dropped.set(true);
        }
    }

    fn check<const N: usize>() {
        let (bytes, report) = Bytes::<N>::new();
        let mut payload = Payload::operation(bytes);
        payload.deliver(Completion::new(N as i32, 0));
        assert_eq!(report.completed.get(), Some(N as i32));
        assert!(report.dropped.get(), "completing consumes the operation");
        assert_eq!(Rc::strong_count(&report), 1);
        std::mem::drop(payload);

        let (bytes, report) = Bytes::<N>::new();
        std::mem::drop(Payload::operation(bytes));
        assert_eq!(report.completed.get(), None);
        assert!(report.dropped.get(), "dropped undelivered");
    }

    #[test]
    fn payloads_of_every_size_complete_and_drop_once() {
        const {
            assert!(Shape::<Option<Bytes<8>>>::FITS);
            assert!(Shape::<Option<Bytes<{ INLINE - 8 }>>>::FITS);
            assert!(!Shape::<Option<Bytes<INLINE>>>::FITS);
        }
        check::<8>();
        check::<{ INLINE - 8 }>();
        check::<INLINE>();
        check::<4096>();
    }

    #[test]
    fn a_payload_is_the_size_of_its_storage_and_a_pointer() {
        assert_eq!(size_of::<Payload>(), INLINE + size_of::<usize>());
    }

    static FLAG_DROPPED: AtomicBool = AtomicBool::new(false);

    /// An operation that notes its drop, and owns nothing: forgetting it
    /// leaks nothing that a leak checker would report.
    struct Flag;

    impl Drop for Flag {
        fn drop(&mut self) {
            FLAG_DROPPED.store(true, Ordering::SeqCst);
        }
    }

    // SAFETY: the entry is never filled in.
    unsafe impl Operation for Flag {
        type Output = ();

        fn prepare(&mut self, _: &mut sys::io_uring_sqe) {}

        fn complete(self, _: Completion) {}
    }

    /// Forgetting a payload, as the driver does with one the kernel may
    /// still use, runs no drop.
    #[test]
    fn a_forgotten_payload_drops_nothing() {
        std::mem::forget(Payload::operation(Flag));
        assert!(!FLAG_DROPPED.load(Ordering::SeqCst));
    }
}
