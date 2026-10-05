//! A channel for many values, from any number of senders to one receiver,
//! which holds as many as are sent.
//!
//! The values wait in a list of blocks of slots. A sender claims a
//! position by moving the tail on, with one atomic add, so that senders
//! sending at once each get a slot of their own and nobody retries; finds
//! the block holding it from the channel's tail block, following the
//! pointers from block to block and installing a new block where the list
//! ends; writes the value into its slot; and publishes it by setting the
//! slot's bit in the block's ready word, by which the receiver, alone at
//! the head, sees that the value is there. Nobody takes a lock, and nobody
//! waits for anyone: a sender that has claimed a slot can always fill it.
//!
//! A block whose values the receiver has all taken goes back to the end of
//! the list for the senders to fill again, so that a channel in a steady
//! state allocates nothing. It goes back only once no sender can reach it:
//! the senders, as they move the tail block on past a full block, leave in
//! it the tail as it was then, and the receiver puts the block back once it
//! has taken every value before that position, by when every sender that
//! could have reached the block has published its value.
//!
//! Closing the channel sets a flag in the tail, so that the senders' claims
//! fall on one side of it or the other: a claim that finds the flag hands
//! its value back, and the receiver takes every value claimed before it.

use std::cell::UnsafeCell;
use std::fmt;
use std::future::poll_fn;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::task::{Context, Poll, ready};

use super::{SendError, TryRecvError};
use crate::coop;
use crate::sync::waker_slot::WakerSlot;

/// How many values a block holds: as many as the ready word has bits for,
/// with one left for `RELEASED`.
const BLOCK_CAP: usize = if usize::BITS >= 64 { 32 } else { 16 };
/// The bits of a position that index its slot in its block; the bits
/// above name the block.
const SLOT_MASK: usize = BLOCK_CAP - 1;
/// The bits of the ready word that say which slots hold a value.
const READY_MASK: usize = (1 << BLOCK_CAP) - 1;
/// Set in the ready word by the sender that moved the tail block on past
/// the block, once it has left in `released_at` the tail as it was then.
const RELEASED: usize = 1 << BLOCK_CAP;
/// Set in the tail once the receiver has closed the channel, or gone: a
/// sender whose claim finds it hands its value back.
const CLOSED: usize = 1 << (usize::BITS - 1);

/// A channel that holds as many values as are sent: sending never waits.
///
/// ```
/// use ruio_executor::sync::mpsc::unbounded_channel;
///
/// # ruio_executor::Runtime::new()?.block_on(async {
/// let (tx, mut rx) = unbounded_channel();
/// let producer = std::thread::spawn(move || {
///     for value in 0..3 {
///         tx.send(value).unwrap();
///     }
/// });
/// let mut received = Vec::new();
/// while let Some(value) = rx.recv().await {
///     received.push(value);
/// }
/// assert_eq!(received, [0, 1, 2]);
/// producer.join().unwrap();
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let block = Block::alloc(0);
    let chan = Arc::new(Chan {
        tail: AtomicUsize::new(0),
        tail_block: AtomicPtr::new(block.as_ptr()),
        receiver_waker: WakerSlot::new(),
        senders: AtomicUsize::new(1),
        head: UnsafeCell::new(Head {
            block,
            index: 0,
            free: block,
            end: None,
        }),
    });
    (
        UnboundedSender {
            chan: Arc::clone(&chan),
        },
        UnboundedReceiver { chan },
    )
}

/// What the halves of a channel share.
struct Chan<T> {
    /// The position the next sender claims, and `CLOSED`.
    tail: AtomicUsize,
    /// The block holding the tail, or one before it, from which a sender
    /// finds its own.
    tail_block: AtomicPtr<Block<T>>,
    /// The receiver's task, waiting for a value.
    receiver_waker: WakerSlot,
    /// How many senders there are.
    senders: AtomicUsize,
    /// The receiver's end of the list: the receiver's alone, borrowed
    /// mutably for every use, until the channel drops what is left.
    head: UnsafeCell<Head<T>>,
}

/// Slots for `BLOCK_CAP` positions in a row, in the list that holds the
/// channel's values.
///
/// A sender reaches a block from the tail block, through the `next`
/// pointers; the receiver, from its head. A block that the receiver has
/// emptied, and that no sender can reach any more, is the receiver's alone
/// to reset and put back at the end of the list: see [`Head::recycle`].
struct Block<T> {
    /// The position of the first slot: set before the block is put in the
    /// list, and unchanged while it is there.
    start: usize,
    /// The block after this one, once a sender or the receiver installs it.
    next: AtomicPtr<Block<T>>,
    /// A bit for each slot, set once its value is written, and `RELEASED`.
    ready: AtomicUsize,
    /// The tail as the tail block moved on past this block: written by the
    /// one sender that moved it, before it sets `RELEASED`, and read by the
    /// receiver after.
    released_at: UnsafeCell<usize>,
    /// Each holds a value from when its bit in `ready` is set until the
    /// receiver takes it.
    slots: [UnsafeCell<MaybeUninit<T>>; BLOCK_CAP],
}

/// The receiver's end of the list.
struct Head<T> {
    /// The block holding `index`, or the one before it.
    block: NonNull<Block<T>>,
    /// The position of the next value to take.
    index: usize,
    /// The first block not yet put back for the senders: `block`, or one
    /// before it.
    free: NonNull<Block<T>>,
    /// The position at which the receiver closed the channel, if it has:
    /// the first at which nothing is sent.
    end: Option<usize>,
}

// SAFETY: a value is written by the one sender that claimed its slot and
// read by the receiver once the ready bit says it is there, so it only
// moves between threads. The head is touched only through the receiver,
// borrowed mutably, until the channel's drop. A block is reached by senders
// through the tail block and the pointers from block to block, and comes
// back to the receiver alone, to be reset and put back, once every sender
// that could reach it has published its value: see `Head::recycle`. The
// rest is atomic.
unsafe impl<T: Send> Send for Chan<T> {}
// SAFETY: as above.
unsafe impl<T: Send> Sync for Chan<T> {}

impl<T> Block<T> {
    /// A new block for the positions from `start`, with nothing in it; made
    /// in place, as a block of a large `T` would not fit the stack.
    #[cold]
    fn alloc(start: usize) -> NonNull<Self> {
        // SAFETY: a block of zeroes is a block with nothing in it: a null
        // `next`, no bit set, position zero, and slots that hold nothing,
        // which `MaybeUninit` may.
        let mut block = unsafe { Box::<Self>::new_zeroed().assume_init() };
        block.start = start;
        NonNull::from(Box::leak(block))
    }

    /// Installs a new block after this one, or finds the one another sender
    /// installed meanwhile.
    #[cold]
    fn grow(&self) -> NonNull<Self> {
        let new = Block::alloc(self.start.wrapping_add(BLOCK_CAP));
        // Release: a sender that follows the pointer sees the block as made.
        // Acquire: the block found instead is seen as its installer left it.
        match self.next.compare_exchange(
            ptr::null_mut(),
            new.as_ptr(),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => new,
            Err(installed) => {
                // SAFETY: nobody has seen the block: it is this sender's to
                // free.
                drop(unsafe { Box::from_raw(new.as_ptr()) });
                NonNull::new(installed).expect("a block, where a null was expected")
            }
        }
    }
}

impl<T> Chan<T> {
    /// The block holding `position`, found from the tail block.
    #[inline]
    fn find_block(&self, position: usize) -> NonNull<Block<T>> {
        // Acquire: the tail block, and the blocks after it, are seen as
        // their installers left them.
        let block = self.tail_block.load(Ordering::Acquire);
        // SAFETY: the pointer only ever holds a block.
        let block = unsafe { NonNull::new_unchecked(block) };
        // SAFETY: the tail block is alive: see `Head::recycle`.
        if unsafe { block.as_ref() }.start == position & !SLOT_MASK {
            return block;
        }
        self.walk_to_block(block, position)
    }

    /// The block holding `position`, found from `block`, the tail block as
    /// loaded, by following the list and growing it where it ends. The tail
    /// block is moved on past the full blocks on the way.
    #[cold]
    fn walk_to_block(&self, mut block: NonNull<Block<T>>, position: usize) -> NonNull<Block<T>> {
        let start = position & !SLOT_MASK;
        // SAFETY: the tail block is alive: see `Head::recycle`.
        let behind = (start - unsafe { block.as_ref() }.start) / BLOCK_CAP;
        // Of the senders filling one block at once, only those whose slots
        // come before the number of blocks the tail block is behind offer
        // to move it, so that few try at once, and more the further it
        // falls behind.
        let mut move_tail = behind > (position & SLOT_MASK);
        loop {
            // SAFETY: a block on the way from the tail block to the one
            // holding the position is alive: see `Head::recycle`.
            let current = unsafe { block.as_ref() };
            if current.start == start {
                return block;
            }
            // Acquire: the block installed is seen as its installer left it.
            let next = match NonNull::new(current.next.load(Ordering::Acquire)) {
                Some(next) => next,
                None => current.grow(),
            };
            // Acquire: the senders that filled the block loaded the tail
            // block before they published their values, so none of them
            // finds it moved past the block and walks on from there in
            // vain. The tail block never passes a block with a slot still
            // to fill.
            move_tail =
                move_tail && current.ready.load(Ordering::Acquire) & READY_MASK == READY_MASK;
            if move_tail {
                // Release: a sender that loads the pointer sees the block it
                // leads to as its installer left it.
                let moved = self.tail_block.compare_exchange(
                    block.as_ptr(),
                    next.as_ptr(),
                    Ordering::Release,
                    Ordering::Relaxed,
                );
                if moved.is_ok() {
                    // Release, with the senders' acquiring claims: a sender
                    // that claims a position from here on sees the tail
                    // block moved, and never reaches this block. Read with
                    // a read-modify-write, so that the claims after it read
                    // from its release sequence.
                    let tail = self.tail.fetch_add(0, Ordering::Release) & !CLOSED;
                    // SAFETY: only the sender that moved the tail block past
                    // this block writes this, once, before `RELEASED`; the
                    // receiver reads it after.
                    unsafe { *current.released_at.get() = tail };
                    // Release: the receiver that sees the flag sees the
                    // position.
                    current.ready.fetch_or(RELEASED, Ordering::Release);
                } else {
                    // Another sender is moving it: this one falls behind.
                    move_tail = false;
                }
            }
            block = next;
        }
    }

    /// Puts `block` back at the end of the list for the senders to fill
    /// again, or frees it if the end is more than a few blocks past the
    /// tail block.
    ///
    /// # Safety
    ///
    /// Nothing else refers to `block`.
    unsafe fn put_back(&self, block: NonNull<Block<T>>) {
        {
            // SAFETY: the caller's: nothing else refers to the block.
            let block = unsafe { &mut *block.as_ptr() };
            *block.next.get_mut() = ptr::null_mut();
            *block.ready.get_mut() = 0;
        }
        // Acquire: the tail block, and the blocks after it, are seen as
        // their installers left them.
        let mut last = self.tail_block.load(Ordering::Acquire);
        for _ in 0..3 {
            // SAFETY: the tail block and the blocks after it are alive: see
            // `Head::recycle`.
            let last_block = unsafe { &*last };
            // SAFETY: the caller's: nothing else refers to the block until
            // the exchange below puts it in the list.
            unsafe { (*block.as_ptr()).start = last_block.start.wrapping_add(BLOCK_CAP) };
            // Release: a sender that follows the pointer sees the block
            // reset. Acquire: the block found instead is seen as its
            // installer left it.
            match last_block.next.compare_exchange(
                ptr::null_mut(),
                block.as_ptr(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(next) => last = next,
            }
        }
        // SAFETY: the caller's: nothing else refers to the block, which
        // `Block::alloc` made.
        drop(unsafe { Box::from_raw(block.as_ptr()) });
    }
}

impl<T> Head<T> {
    /// Takes the value at `index`, if it is there.
    #[inline]
    fn pop(&mut self) -> Option<T> {
        let start = self.index & !SLOT_MASK;
        // SAFETY: the blocks from `free` on are alive, and `block` is one of
        // them: see `recycle`.
        let mut block = unsafe { self.block.as_ref() };
        while block.start != start {
            // Acquire: the block installed is seen as its installer left it.
            self.block = NonNull::new(block.next.load(Ordering::Acquire))?;
            // SAFETY: as above.
            block = unsafe { self.block.as_ref() };
        }
        let offset = self.index & SLOT_MASK;
        // Acquire: the value the bit says is there is seen.
        if block.ready.load(Ordering::Acquire) & (1 << offset) == 0 {
            return None;
        }
        // SAFETY: the bit says the slot holds a value, which the receiver
        // alone reads, once: the index, moved on below, never comes back.
        let slot = unsafe { &*block.slots[offset].get() };
        // SAFETY: as above.
        let value = unsafe { slot.assume_init_read() };
        self.index += 1;
        Some(value)
    }

    /// Puts back, for the senders to fill again, the blocks before `block`
    /// that no sender can reach any more.
    ///
    /// Every value in a block before `block` has been taken. A sender
    /// reaches a block from the tail block: it claims a position, loads the
    /// tail block, and follows the list from there to its own block. Once
    /// the tail block has moved on past a block, the sender that moved it
    /// leaves the tail as it was then in `released_at` and sets `RELEASED`.
    /// A sender that claims a position from there on sees the tail block
    /// moved (see [`Chan::walk_to_block`]); one that claimed an earlier
    /// position has published its value, and so is done with every block
    /// on its way, by the time the receiver has taken every value before
    /// `released_at`. From then on nothing but the receiver reaches the
    /// block, and so nothing but the receiver reaches a block before
    /// `free`, which it resets and puts back as it comes to it.
    fn recycle(&mut self, chan: &Chan<T>) {
        while self.free != self.block {
            // SAFETY: the blocks from `free` on are alive, as above.
            let block = unsafe { self.free.as_ref() };
            // Acquire: the position written before `RELEASED` is seen.
            if block.ready.load(Ordering::Acquire) & RELEASED == 0 {
                return;
            }
            // SAFETY: `RELEASED` says the position was written, and nothing
            // writes it again.
            if unsafe { *block.released_at.get() } > self.index {
                return;
            }
            // Relaxed: the pointer was acquired as the head moved past the
            // block, and nothing has changed it since.
            let next = block.next.load(Ordering::Relaxed);
            let recycled = self.free;
            self.free = NonNull::new(next).expect("the head moved past the block through it");
            // SAFETY: nothing else reaches the block, as above.
            unsafe { chan.put_back(recycled) };
        }
    }
}

impl<T> Drop for Chan<T> {
    fn drop(&mut self) {
        let head = self.head.get_mut();
        // Every value left is written: each sender's sends were done before
        // it went. Dropped one by one: a value's drop can run any code.
        while let Some(value) = head.pop() {
            drop(value);
        }
        let mut block = Some(head.free);
        while let Some(current) = block {
            // SAFETY: the blocks from `free` on are alive, `Block::alloc`
            // made each, and the channel, which outlives both halves, is
            // the last to hold them.
            let current = unsafe { Box::from_raw(current.as_ptr()) };
            // Relaxed: nothing else is left to publish anything.
            block = NonNull::new(current.next.load(Ordering::Relaxed));
        }
    }
}

/// Sends values into the channel, without waiting. Clones send into the
/// same channel; once every one is dropped, the receiver sees the channel
/// end.
pub struct UnboundedSender<T> {
    chan: Arc<Chan<T>>,
}

impl<T> UnboundedSender<T> {
    /// Sends `value`, or hands it back if the receiver is gone or has closed
    /// the channel. It never waits: the channel grows to hold whatever is
    /// sent.
    ///
    /// # Errors
    ///
    /// [`SendError`] with the value, if the receiver is gone or has closed
    /// the channel.
    #[inline]
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let chan = &*self.chan;
        // Acquire: the move of the tail block by a sender that read the tail
        // before this claim is seen, so that no block it passed, which may be
        // gone by now, is reached: see `Chan::walk_to_block`.
        let position = chan.tail.fetch_add(1, Ordering::Acquire);
        if position & CLOSED != 0 {
            return Err(SendError(value));
        }
        let block = chan.find_block(position);
        // SAFETY: the block holds the position, whose value is not yet in,
        // so it is not put back before this is done: see `Head::recycle`.
        let block = unsafe { block.as_ref() };
        let offset = position & SLOT_MASK;
        // SAFETY: the claim makes this the slot's one writer, until the
        // ready bit hands it to the receiver.
        unsafe { (*block.slots[offset].get()).write(value) };
        // Release: the receiver that sees the bit sees the value. From here
        // on the block may be put back or freed.
        block.ready.fetch_or(1 << offset, Ordering::Release);
        chan.receiver_waker.wake();
        Ok(())
    }

    /// Whether the receiver is gone or has closed the channel, so that
    /// sending is in vain.
    pub fn is_closed(&self) -> bool {
        // Relaxed: the flag is all that is read.
        self.chan.tail.load(Ordering::Relaxed) & CLOSED != 0
    }
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        // Relaxed: this sender keeps the count above zero meanwhile.
        self.chan.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            chan: Arc::clone(&self.chan),
        }
    }
}

impl<T> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        // Release: the receiver that sees the count reach zero sees every
        // value sent; acquire, so that the last sender's wake comes after
        // the others' values too.
        if self.chan.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.chan.receiver_waker.wake();
        }
    }
}

impl<T> fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedSender").finish_non_exhaustive()
    }
}

/// Receives the channel's values, in the order they were sent.
pub struct UnboundedReceiver<T> {
    chan: Arc<Chan<T>>,
}

impl<T> UnboundedReceiver<T> {
    /// The next value, waiting for one, or `None` once the channel has
    /// ended: every sender is gone, or the receiver closed it, and every
    /// value sent before has been taken.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// The next value, or `None` once the channel has ended, as for
    /// [`recv`](Self::recv); arranges for the task to be woken otherwise,
    /// or, once its budget is spent, wakes it at once.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let proceed = ready!(coop::poll_proceed(cx));
        let value = match self.take(Some(cx)) {
            Ok(value) => Some(value),
            Err(TryRecvError::Closed) => None,
            Err(TryRecvError::Empty) => return Poll::Pending,
        };
        proceed.made_progress();
        Poll::Ready(value)
    }

    /// The next value if there is one, without waiting.
    ///
    /// # Errors
    ///
    /// [`TryRecvError::Empty`] if the channel is empty, and
    /// [`TryRecvError::Closed`] if it has ended as well: every sender is
    /// gone, or the receiver closed it.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.take(None)
    }

    /// Closes the channel: a send from now on fails and hands its value
    /// back, while every value sent before stays to be taken, after which
    /// [`recv`](Self::recv) gives `None`, senders or no senders.
    pub fn close(&mut self) {
        let chan = &*self.chan;
        // Relaxed: the flag reaches the senders in the positions they
        // claim, which the tail orders with this; the values before it
        // are published by their ready bits.
        let tail = chan.tail.fetch_or(CLOSED, Ordering::Relaxed);
        if tail & CLOSED == 0 {
            self.head().end = Some(tail);
        }
    }

    /// The receiver's end of the list.
    #[inline]
    fn head(&mut self) -> &mut Head<T> {
        // SAFETY: the receiver, borrowed mutably, is the head's one user
        // until the channel drops what is left.
        unsafe { &mut *self.chan.head.get() }
    }

    /// Takes the next value; if there is none, and a context is given,
    /// registers its waker and looks again, so that no send is missed.
    fn take(&mut self, cx: Option<&Context<'_>>) -> Result<T, TryRecvError> {
        if let Some(value) = self.pop() {
            return Ok(value);
        }
        if let Some(cx) = cx {
            // SAFETY: the receiver, borrowed mutably, is the slot's one
            // owner.
            unsafe { self.chan.receiver_waker.register(cx.waker()) };
            if let Some(value) = self.pop() {
                return Ok(value);
            }
        }
        // Acquire: with no sender left, every value sent is seen.
        if self.chan.senders.load(Ordering::Acquire) == 0 {
            return self.pop().ok_or(TryRecvError::Closed);
        }
        // Closed, the channel ends once every position claimed before the
        // close has been taken; a sender still filling one wakes the
        // receiver as it is done.
        let head = self.head();
        if head.end == Some(head.index) {
            return Err(TryRecvError::Closed);
        }
        Err(TryRecvError::Empty)
    }

    /// Takes the value at the head, if it is there, and puts back the
    /// blocks the senders can fill again.
    #[inline]
    fn pop(&mut self) -> Option<T> {
        let chan = &*self.chan;
        // SAFETY: the receiver, borrowed mutably, is the head's one user
        // until the channel drops what is left.
        let head = unsafe { &mut *chan.head.get() };
        let value = head.pop()?;
        if head.free != head.block {
            head.recycle(chan);
        }
        Some(value)
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        // Relaxed: as in `close`.
        self.chan.tail.fetch_or(CLOSED, Ordering::Relaxed);
        // Dropped one by one, holding nothing: a value's drop can run any
        // code. A value claimed before the close and not yet written is
        // left for the channel to drop.
        while let Some(value) = self.pop() {
            drop(value);
        }
    }
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver").finish_non_exhaustive()
    }
}
