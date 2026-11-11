//! Pools of buffers that operations take from as data arrives.
//!
//! A completion port has no buffers for the system to pick from, as
//! io_uring's provided-buffer rings are, so a pool here is the thread's: a
//! reader waits for its handle to have data, takes a buffer of the pool, and
//! reads into it. Until then it holds none, so that many idle readers share
//! a few buffers, as they do on io_uring.
//!
//! When every buffer is leased, readers wait in a line, and a buffer that
//! returns is handed to the first of them, rather than left for all of them
//! to ask for, or for one that came later to take. A reader dropped before
//! it takes the buffer it was handed leaves the buffer to the next.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::iter;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::slice;
use std::task::{Context, Poll, Waker};
use std::vec;

use ruio_buf::{IoBuf, IoBufMut, IoBufs};

/// A pool of equal-sized buffers that the operations reading into it take
/// from once there is data to read.
///
/// An operation reading into a pool claims no buffer until data arrives, so
/// many idle readers can share a few buffers. The data comes back as a
/// [`BufLease`], which returns its buffer to the pool when dropped, or as a
/// [`BufBundle`] of them. When every buffer is leased, a read into the pool
/// waits for one to return.
///
/// The buffers are allocated, zeroed, as they are first needed, and freed
/// once the pool and every lease are gone. A pool belongs to the thread it
/// was made on.
#[derive(Clone)]
pub struct BufPool {
    pool: Rc<Pool>,
}

struct Pool {
    count: u16,
    size: u32,
    /// How many buffers have been allocated: those leased, those handed to
    /// readers, and those in `free`.
    allocated: Cell<u16>,
    /// The allocated buffers that nothing holds, the last to return last.
    /// It is empty while a reader waits in the line.
    free: RefCell<Vec<Box<[u8]>>>,
    /// Who waits for a buffer to return, in the order they came.
    line: RefCell<VecDeque<Waiting>>,
    /// How many readers in the line were dropped as they waited. The place
    /// of one stays where it is, to be passed over when its turn comes.
    gone: Cell<usize>,
}

/// One who waits in a pool's line.
enum Waiting {
    /// A task that asked whether a buffer was available.
    Task(Waker),
    /// A reader, by its place.
    Reader(Rc<Place>),
}

/// A reader's place in a pool's line, which the reader and the line share.
struct Place {
    /// Whether the reader waits in the line.
    waiting: Cell<bool>,
    /// The waker of the reader's task, while the reader waits.
    waker: Cell<Option<Waker>>,
    /// The buffer that returned when the reader was the first in the line,
    /// until the reader takes it.
    handed: Cell<Option<Box<[u8]>>>,
}

impl BufPool {
    /// A pool of `count` buffers of `size` bytes each.
    ///
    /// The pool needs no runtime, though io_uring's, which is registered
    /// with a ring, does.
    ///
    /// # Errors
    ///
    /// `InvalidInput` unless `count` is a power of two up to 32768 and
    /// `size` is above zero, which is what io_uring's pools take.
    pub fn new(count: u16, size: u32) -> io::Result<Self> {
        if !count.is_power_of_two() || count > 1 << 15 || size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a pool needs a power-of-two count up to 32768 and a size above zero",
            ));
        }
        Ok(Self {
            pool: Rc::new(Pool {
                count,
                size,
                allocated: Cell::new(0),
                free: RefCell::new(Vec::new()),
                line: RefCell::new(VecDeque::new()),
                gone: Cell::new(0),
            }),
        })
    }

    /// The number of buffers.
    #[inline]
    pub fn count(&self) -> usize {
        usize::from(self.pool.count)
    }

    /// The size of each buffer.
    #[inline]
    pub fn buffer_size(&self) -> usize {
        self.pool.size as usize
    }

    /// The number of buffers that are not leased.
    #[inline]
    pub fn available(&self) -> usize {
        self.pool.available()
    }

    /// Whether a buffer is available; if not, arranges for the task to be
    /// woken when one returns. The buffer is no longer available by the
    /// time the task runs if a reader waited for it, or took it since.
    pub fn poll_available(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.available() > 0 {
            return Poll::Ready(());
        }
        // A task that asks again before it is woken is still at the back.
        // One that asks again later is woken once for each time it asked.
        let asked = matches!(
            self.pool.line.borrow().back(),
            Some(Waiting::Task(waker)) if waker.will_wake(cx.waker())
        );
        if !asked {
            // Cloned outside the borrow, as cloning runs code of the task's.
            let waker = cx.waker().clone();
            self.pool.line.borrow_mut().push_back(Waiting::Task(waker));
        }
        Poll::Pending
    }

    /// A buffer, as a lease that holds no data yet, unless every buffer is
    /// leased or handed to a reader.
    pub(crate) fn take(&self) -> Option<BufLease> {
        let pool = &self.pool;
        let free = pool.free.borrow_mut().pop();
        let buf = match free {
            Some(buf) => buf,
            None => {
                let allocated = pool.allocated.get();
                if allocated == pool.count {
                    return None;
                }
                pool.allocated.set(allocated + 1);
                vec![0; pool.size as usize].into_boxed_slice()
            }
        };
        Some(self.lease(buf))
    }

    /// The lease of `buf`, a buffer of the pool's, which holds no data yet.
    fn lease(&self, buf: Box<[u8]>) -> BufLease {
        BufLease {
            buf,
            len: 0,
            pool: Rc::clone(&self.pool),
        }
    }
}

impl Pool {
    /// How many buffers nothing holds: those to allocate yet, and those
    /// that returned with no reader waiting.
    fn available(&self) -> usize {
        usize::from(self.count - self.allocated.get()) + self.free.borrow().len()
    }

    /// Takes back a buffer: hands it to the first reader that waits in the
    /// line, or keeps it if none does, and wakes the tasks that asked
    /// before either.
    fn put(&self, buf: Box<[u8]>) {
        let mut tasks = Vec::new();
        let reader = loop {
            let next = self.line.borrow_mut().pop_front();
            match next {
                None => break None,
                Some(Waiting::Task(waker)) => tasks.push(waker),
                Some(Waiting::Reader(place)) => {
                    if place.waiting.replace(false) {
                        break Some(place);
                    }
                    self.gone.set(self.gone.get() - 1);
                }
            }
        };
        let waker = match reader {
            Some(place) => {
                place.handed.set(Some(buf));
                place.waker.take()
            }
            None => {
                self.free.borrow_mut().push(buf);
                None
            }
        };
        // Waking runs code of a task's, which may ask for a buffer, so the
        // buffer is where it goes by now, and the line is not borrowed.
        for task in tasks {
            task.wake();
        }
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Counts a reader that was dropped as it waited in the line, and
    /// clears the line of the places of such readers once they are most of
    /// it, so that it does not grow while no buffer returns.
    fn left(&self) {
        let gone = self.gone.get() + 1;
        let mut line = self.line.borrow_mut();
        if gone <= line.len() / 2 {
            self.gone.set(gone);
            return;
        }
        // A reader that was dropped took its waker with it, so dropping its
        // place here runs no code of a task's.
        line.retain(|waiting| match waiting {
            Waiting::Task(_) => true,
            Waiting::Reader(place) => place.waiting.get(),
        });
        self.gone.set(0);
    }
}

impl fmt::Debug for BufPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufPool")
            .field("count", &self.count())
            .field("buffer_size", &self.buffer_size())
            .field("available", &self.available())
            .finish_non_exhaustive()
    }
}

/// A reader's claim to a buffer of a pool, and its place in the pool's
/// line while it waits for one.
pub(crate) struct Claim {
    pool: BufPool,
    /// The reader's place, made the first time it has to wait.
    place: Option<Rc<Place>>,
}

impl Claim {
    /// The claim of a reader into `pool`.
    pub(crate) fn new(pool: &BufPool) -> Self {
        Self {
            pool: pool.clone(),
            place: None,
        }
    }

    /// A buffer, as a lease that holds no data yet: the one the reader was
    /// handed as it waited, or one of the pool's, if it has one.
    pub(crate) fn take(&mut self) -> Option<BufLease> {
        let handed = self.place.as_ref().and_then(|place| place.handed.take());
        match handed {
            Some(buf) => Some(self.pool.lease(buf)),
            None => self.pool.take(),
        }
    }

    /// A buffer, as a lease that holds no data yet, once there is one for
    /// the reader. Until then the reader waits in the pool's line, and its
    /// task is woken when it is handed a buffer.
    pub(crate) fn poll_take(&mut self, cx: &mut Context<'_>) -> Poll<BufLease> {
        if let Some(lease) = self.take() {
            return Poll::Ready(lease);
        }
        let place = self.place.get_or_insert_with(|| {
            Rc::new(Place {
                waiting: Cell::new(false),
                waker: Cell::new(None),
                handed: Cell::new(None),
            })
        });
        let waker = match place.waker.take() {
            Some(waker) if waker.will_wake(cx.waker()) => waker,
            _ => cx.waker().clone(),
        };
        place.waker.set(Some(waker));
        if !place.waiting.replace(true) {
            let waiting = Waiting::Reader(Rc::clone(place));
            self.pool.pool.line.borrow_mut().push_back(waiting);
        }
        Poll::Pending
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        let Some(place) = self.place.take() else {
            return;
        };
        drop(place.waker.take());
        if place.waiting.replace(false) {
            self.pool.pool.left();
        } else if let Some(buf) = place.handed.take() {
            // The buffer the reader was handed is the next one's.
            self.pool.pool.put(buf);
        }
    }
}

/// Data read into a pooled buffer. The buffer returns to its [`BufPool`]
/// when the lease is dropped.
///
/// A lease is also a buffer of its own, to write its data out, or read more
/// into, up to its [`capacity`](BufLease::capacity).
pub struct BufLease {
    /// The buffer, all of it initialized.
    buf: Box<[u8]>,
    len: usize,
    pool: Rc<Pool>,
}

impl BufLease {
    /// How many bytes of data the lease holds.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the lease holds no data.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// How much the lease can hold: the size of the buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Reads into the buffer with `read`, which is given all of it and
    /// says how many bytes it put at its start: the data the lease then
    /// holds.
    pub(crate) fn fill(
        &mut self,
        read: impl FnOnce(&mut [u8]) -> io::Result<usize>,
    ) -> io::Result<usize> {
        let len = read(&mut self.buf)?.min(self.buf.len());
        self.len = len;
        Ok(len)
    }
}

impl Deref for BufLease {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl DerefMut for BufLease {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.buf[..self.len]
    }
}

// SAFETY: the buffer is on the heap, which moving the lease leaves in
// place, and is the lease's alone until it is dropped. All of it is
// initialized.
unsafe impl IoBuf for BufLease {
    #[inline]
    fn stable_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    #[inline]
    fn bytes_init(&self) -> usize {
        self.len
    }

    #[inline]
    fn bytes_total(&self) -> usize {
        self.capacity()
    }
}

// SAFETY: as for `IoBuf`; `set_init` only moves the length within the
// buffer, all of which is initialized.
unsafe impl IoBufMut for BufLease {
    #[inline]
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }

    #[inline]
    unsafe fn set_init(&mut self, pos: usize) {
        self.len = self.len.max(pos.min(self.capacity()));
    }
}

impl Drop for BufLease {
    #[inline]
    fn drop(&mut self) {
        self.pool.put(mem::take(&mut self.buf));
    }
}

impl fmt::Debug for BufLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufLease")
            .field("len", &self.len)
            .field("capacity", &self.capacity())
            .finish()
    }
}

/// The data of one receive into a pool: the leases of the buffers it
/// filled, in order.
///
/// A receive here fills one buffer; bundles [`append`](BufBundle::append)ed
/// to one another hold more. The leases return their buffers to the pool
/// when dropped, whether together or apart.
pub struct BufBundle {
    first: BufLease,
    rest: Vec<BufLease>,
}

impl BufBundle {
    /// The bundle of a receive that filled one buffer.
    pub(crate) fn of(first: BufLease) -> Self {
        Self {
            first,
            rest: Vec::new(),
        }
    }

    /// How many bytes of data the bundle holds.
    #[inline]
    pub fn len(&self) -> usize {
        self.iter().map(BufLease::len).sum()
    }

    /// Whether the bundle holds no data.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many buffers the data fills.
    #[inline]
    pub fn buffers(&self) -> usize {
        1 + self.rest.len()
    }

    /// The leases, in order.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &BufLease> {
        iter::once(&self.first).chain(&self.rest)
    }

    /// Adds `other`'s leases after this bundle's, as for data that arrived
    /// after it.
    #[inline]
    pub fn append(&mut self, other: BufBundle) {
        self.rest.reserve(other.buffers());
        self.rest.push(other.first);
        self.rest.extend(other.rest);
    }

    /// The data, copied out into one vector.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.len());
        for lease in self {
            data.extend_from_slice(lease);
        }
        data
    }
}

// SAFETY: each lease is an `IoBuf`, whose memory is on the heap wherever
// the lease is.
unsafe impl IoBufs for BufBundle {
    #[inline]
    fn buf_count(&self) -> usize {
        self.buffers()
    }

    #[inline]
    fn buf(&self, index: usize) -> (*const u8, usize) {
        let lease = match index {
            0 => &self.first,
            _ => &self.rest[index - 1],
        };
        (lease.stable_ptr(), lease.bytes_init())
    }
}

impl IntoIterator for BufBundle {
    type Item = BufLease;
    type IntoIter = iter::Chain<iter::Once<BufLease>, vec::IntoIter<BufLease>>;

    fn into_iter(self) -> Self::IntoIter {
        iter::once(self.first).chain(self.rest)
    }
}

impl<'a> IntoIterator for &'a BufBundle {
    type Item = &'a BufLease;
    type IntoIter = iter::Chain<iter::Once<&'a BufLease>, slice::Iter<'a, BufLease>>;

    fn into_iter(self) -> Self::IntoIter {
        iter::once(&self.first).chain(&self.rest)
    }
}

impl fmt::Debug for BufBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self).finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use super::*;

    /// A task's waker, which counts how often the task was woken.
    struct Woken(AtomicUsize);

    impl Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Woken {
        fn times(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn task() -> (Arc<Woken>, Waker) {
        let woken = Arc::new(Woken(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&woken));
        (woken, waker)
    }

    /// Whether `claim` has to wait for a buffer, as polled by the task of
    /// `waker`.
    fn waits(claim: &mut Claim, waker: &Waker) -> bool {
        claim
            .poll_take(&mut Context::from_waker(waker))
            .is_pending()
    }

    /// Puts `data` at the start of `lease`, as an operation that reads
    /// into it does.
    fn read_into(lease: &mut BufLease, data: &[u8]) {
        assert!(data.len() <= lease.bytes_total());
        // SAFETY: the lease's memory is writable for `bytes_total` bytes,
        // which the data fits in, and the data lies elsewhere.
        unsafe {
            lease
                .stable_mut_ptr()
                .copy_from_nonoverlapping(data.as_ptr(), data.len());
        }
        // SAFETY: that many bytes were just written, within the buffer.
        unsafe { lease.set_init(data.len()) };
    }

    fn lease_of(pool: &BufPool, data: &[u8]) -> BufLease {
        let mut lease = pool.take().expect("the pool has a buffer");
        read_into(&mut lease, data);
        lease
    }

    #[test]
    fn a_pool_takes_what_io_urings_take() {
        for (count, size) in [(0, 16), (3, 16), (1 << 15 | 1, 16), (4, 0)] {
            let err = BufPool::new(count, size).expect_err("an invalid pool");
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        }
        let pool = BufPool::new(1 << 15, 1 << 20).expect("the largest count");
        assert_eq!(pool.count(), 1 << 15);
        assert_eq!(pool.buffer_size(), 1 << 20);
        assert_eq!(pool.available(), 1 << 15);
    }

    #[test]
    fn leases_return_their_buffers() {
        let pool = BufPool::new(2, 8).expect("a pool");
        let first = pool.take().expect("the first buffer");
        let second = pool.take().expect("the second buffer");
        assert_eq!(pool.available(), 0);
        assert!(pool.take().is_none());
        drop(first);
        assert_eq!(pool.available(), 1);
        // The pool itself may go before its leases.
        let again = pool.clone();
        drop(pool);
        drop(second);
        assert_eq!(again.available(), 2);
        assert!(format!("{again:?}").contains("available: 2"));
    }

    #[test]
    fn a_lease_holds_what_was_read_into_it() {
        let pool = BufPool::new(1, 8).expect("a pool");
        let mut lease = pool.take().expect("a buffer");
        assert!(lease.is_empty());
        assert_eq!(lease.capacity(), 8);
        assert_eq!((lease.bytes_init(), lease.bytes_total()), (0, 8));
        read_into(&mut lease, b"abc");
        assert_eq!(&*lease, b"abc");
        assert_eq!((lease.len(), lease.bytes_init()), (3, 3));
        lease[0] = b'x';
        assert_eq!(&*lease, b"xbc");
        // SAFETY: the buffer is initialized throughout, and holds 8 bytes.
        unsafe { lease.set_init(64) };
        assert_eq!(lease.len(), 8);
        assert_eq!(format!("{lease:?}"), "BufLease { len: 8, capacity: 8 }");
        drop(lease);
        // The next lease of the buffer holds none of it.
        assert!(pool.take().expect("the buffer again").is_empty());
    }

    #[test]
    fn a_lease_holds_what_a_read_put_in_it() {
        let pool = BufPool::new(1, 8).expect("a pool");
        let mut lease = pool.take().expect("a buffer");
        let mut data: &[u8] = b"hello, pool";
        let read = lease.fill(|buf| io::Read::read(&mut data, buf));
        assert_eq!(read.expect("a read of a slice"), 8);
        assert_eq!(&*lease, b"hello, p");
        // The next read's data takes the place of the last one's.
        let read = lease.fill(|buf| io::Read::read(&mut data, buf));
        assert_eq!(read.expect("a read of a slice"), 3);
        assert_eq!(&*lease, b"ool");
        // A read that fails leaves the lease as it was.
        let err = lease
            .fill(|_| Err(io::ErrorKind::WouldBlock.into()))
            .expect_err("a read that fails");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(&*lease, b"ool");
        // One that says it read more than the buffer holds is not believed.
        assert_eq!(lease.fill(|_| Ok(64)).expect("a read"), 8);
        assert_eq!(lease.len(), 8);
    }

    #[test]
    fn a_buffer_that_returns_is_handed_to_the_first_reader_in_line() {
        let pool = BufPool::new(1, 8).expect("a pool");
        let lease = pool.take().expect("the buffer");
        let (mut first, mut second) = (Claim::new(&pool), Claim::new(&pool));
        let ((woken_first, task_first), (woken_second, task_second)) = (task(), task());
        assert!(first.take().is_none());
        assert!(waits(&mut first, &task_first));
        assert!(waits(&mut second, &task_second));
        // Asking again leaves a reader where it is.
        assert!(waits(&mut first, &task_first));
        assert_eq!(pool.pool.line.borrow().len(), 2);

        drop(lease);
        assert_eq!((woken_first.times(), woken_second.times()), (1, 0));
        // The buffer is the first reader's, whoever asks before it runs.
        assert_eq!(pool.available(), 0);
        assert!(pool.take().is_none());
        assert!(second.take().is_none());
        assert!(waits(&mut second, &task_second));
        let lease = first.take().expect("the buffer it was handed");
        assert!(lease.is_empty());

        drop(lease);
        assert_eq!((woken_first.times(), woken_second.times()), (1, 1));
        assert!(!waits(&mut second, &task_second));
        // With nobody waiting, the buffer is the pool's again.
        assert_eq!(pool.available(), 1);
        assert!(!waits(&mut first, &task_first));
    }

    #[test]
    fn a_reader_dropped_with_the_buffer_it_was_handed_leaves_it_to_the_next() {
        let pool = BufPool::new(1, 8).expect("a pool");
        let lease = pool.take().expect("the buffer");
        let (mut first, mut second) = (Claim::new(&pool), Claim::new(&pool));
        let ((woken_first, task_first), (woken_second, task_second)) = (task(), task());
        assert!(waits(&mut first, &task_first));
        assert!(waits(&mut second, &task_second));

        drop(lease);
        assert_eq!((woken_first.times(), woken_second.times()), (1, 0));
        drop(first);
        assert_eq!(woken_second.times(), 1);
        assert_eq!(pool.available(), 0);
        // The last to hold it leaves it to the pool.
        drop(second);
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn a_reader_dropped_as_it_waits_is_passed_over() {
        let pool = BufPool::new(1, 8).expect("a pool");
        let lease = pool.take().expect("the buffer");
        let mut claims: Vec<Claim> = (0..5).map(|_| Claim::new(&pool)).collect();
        let tasks: Vec<_> = (0..5).map(|_| task()).collect();
        for (claim, (_, waker)) in claims.iter_mut().zip(&tasks) {
            assert!(waits(claim, waker));
        }
        // The first two are dropped, and their places stay in the line,
        // of which they are not yet most.
        drop(claims.drain(..2));
        assert_eq!(pool.pool.line.borrow().len(), 5);
        assert_eq!(pool.pool.gone.get(), 2);

        drop(lease);
        let woken: Vec<usize> = tasks.iter().map(|(woken, _)| woken.times()).collect();
        assert_eq!(woken, [0, 0, 1, 0, 0]);
        assert_eq!(pool.pool.line.borrow().len(), 2);
        assert_eq!(pool.pool.gone.get(), 0);

        // Of the two left in the line, one dropped is not most of it, and
        // two are, which clears it.
        drop(claims.pop());
        assert_eq!(pool.pool.line.borrow().len(), 2);
        assert_eq!(pool.pool.gone.get(), 1);
        drop(claims.pop());
        assert_eq!(pool.pool.line.borrow().len(), 0);
        assert_eq!(pool.pool.gone.get(), 0);

        let lease = claims[0].take().expect("the buffer it was handed");
        drop(lease);
        assert_eq!(pool.available(), 1);
        let woken: Vec<usize> = tasks.iter().map(|(woken, _)| woken.times()).collect();
        assert_eq!(woken, [0, 0, 1, 0, 0]);
    }

    #[test]
    fn readers_are_handed_buffers_in_the_order_they_came() {
        let pool = BufPool::new(2, 8).expect("a pool");
        let mut leases = vec![pool.take(), pool.take()];
        let mut claims: Vec<Claim> = (0..3).map(|_| Claim::new(&pool)).collect();
        let tasks: Vec<_> = (0..3).map(|_| task()).collect();
        for (claim, (_, waker)) in claims.iter_mut().zip(&tasks) {
            assert!(waits(claim, waker));
        }
        // Each reader gives back the buffer it is handed, which goes to the
        // one after it, though the reader waits again at once.
        leases.clear();
        for turn in 0..9 {
            let reader = turn % 3;
            let lease = claims[reader].take().expect("the buffer it was handed");
            assert!(waits(&mut claims[reader], &tasks[reader].1));
            drop(lease);
        }
        let woken: Vec<usize> = tasks.iter().map(|(woken, _)| woken.times()).collect();
        assert_eq!(woken, [4, 4, 3]);
        assert_eq!(pool.available(), 0);
        drop(claims);
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn tasks_that_asked_are_woken_when_a_buffer_returns() {
        let pool = BufPool::new(1, 8).expect("a pool");
        let ((woken_task, waker_task), (woken_reader, waker_reader)) = (task(), task());
        let asks = |pool: &BufPool| pool.poll_available(&mut Context::from_waker(&waker_task));
        assert!(asks(&pool).is_ready());

        let lease = pool.take().expect("the buffer");
        // A task that asks again before it is woken is in the line once,
        // where its waker can be told to be the one there already, which
        // `will_wake` need not tell.
        let asked = if waker_task.will_wake(&waker_task.clone()) {
            1
        } else {
            2
        };
        assert!(asks(&pool).is_pending());
        assert!(asks(&pool).is_pending());
        assert_eq!(pool.pool.line.borrow().len(), asked);
        let mut reader = Claim::new(&pool);
        assert!(waits(&mut reader, &waker_reader));

        // The buffer goes to the reader behind the task, which finds none.
        drop(lease);
        assert_eq!((woken_task.times(), woken_reader.times()), (asked, 1));
        assert!(asks(&pool).is_pending());
        drop(reader.take().expect("the buffer it was handed"));
        assert_eq!((woken_task.times(), woken_reader.times()), (asked + 1, 1));
        assert!(asks(&pool).is_ready());
    }

    #[test]
    fn a_bundle_is_its_leases_in_order() {
        let pool = BufPool::new(4, 4).expect("a pool");
        let mut bundle = BufBundle::of(lease_of(&pool, b"ab"));
        assert_eq!((bundle.len(), bundle.buffers()), (2, 1));
        assert!(!bundle.is_empty());
        let mut more = BufBundle::of(lease_of(&pool, b"cdef"));
        more.append(BufBundle::of(lease_of(&pool, b"g")));
        bundle.append(more);

        assert_eq!((bundle.len(), bundle.buffers()), (7, 3));
        assert_eq!(bundle.to_vec(), b"abcdefg");
        assert_eq!((bundle.buf_count(), bundle.bytes_init()), (3, 7));
        let (ptr, len) = bundle.buf(1);
        // SAFETY: the bundle says its second buffer holds that many bytes
        // there, and it is not changed meanwhile.
        assert_eq!(unsafe { slice::from_raw_parts(ptr, len) }, b"cdef");
        let lens: Vec<usize> = bundle.iter().map(BufLease::len).collect();
        assert_eq!(lens, [2, 4, 1]);
        assert!(format!("{bundle:?}").starts_with("[BufLease { len: 2"));

        assert_eq!(pool.available(), 1);
        let mut leases = bundle.into_iter();
        drop(leases.next());
        assert_eq!(pool.available(), 2);
        drop(leases);
        assert_eq!(pool.available(), 4);
        assert!(BufBundle::of(pool.take().expect("a buffer")).is_empty());
    }
}
