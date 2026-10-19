//! The portable `AsyncFd`: readiness waits with tokio's API, `Send` and
//! `Sync`, on the ring of the thread that polls them.

mod support;

use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::io::{self, ErrorKind, Read as _, Write as _};
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use ruio::io::portable::{AsyncFd, AsyncFdReadyGuard, AsyncFdReadyMutGuard, Interest, Ready};

use support::within;

fn assert_send<T: Send>(_: &T) {}

fn assert_send_sync<T: Send + Sync>(_: &T) {}

/// A connected pair of nonblocking stream sockets.
fn pair() -> (UnixStream, UnixStream) {
    let (a, b) = UnixStream::pair().unwrap();
    a.set_nonblocking(true).unwrap();
    b.set_nonblocking(true).unwrap();
    (a, b)
}

/// Two cores, on two CPUs if the machine has them.
fn two() -> ruio::Cores {
    let all = ruio::Cores::all().unwrap();
    ruio::Cores::cpus(all.cpu_list().iter().copied().cycle().take(2))
}

/// Polls `future` once, where it lies, and says whether it is pending.
async fn pending_once<F: Future + Unpin>(future: &mut F) -> bool {
    poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx).is_pending())).await
}

/// How many operations the thread's ring has queued.
fn operations() -> u64 {
    ruio::driver().stats().operations
}

/// Reads everything `stream` has, until a read would block.
fn drain(mut stream: &UnixStream) -> usize {
    let mut buf = [0; 64 << 10];
    let mut total = 0;
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return total,
            Ok(read) => total += read,
            Err(err) if err.kind() == ErrorKind::WouldBlock => return total,
            Err(err) => panic!("{err}"),
        }
    }
}

#[ruio::test]
async fn readable_waits_for_data_and_try_io_reads_it() -> io::Result<()> {
    let (a, mut b) = pair();
    let a = AsyncFd::new(a)?;
    let waited = ruio::time::timeout(Duration::from_millis(20), a.readable()).await;
    assert!(waited.is_err(), "an empty socket is not readable");
    b.write_all(b"hello")?;
    let mut guard = within(a.readable()).await?;
    assert!(guard.ready().is_readable());
    assert!(!guard.ready().is_read_closed());
    let mut buf = [0; 16];
    let read = guard.try_io(|fd| fd.get_ref().read(&mut buf));
    assert_eq!(read.unwrap()?, 5);
    assert_eq!(&buf[..5], b"hello");
    Ok(())
}

#[ruio::test]
async fn a_wait_that_polls_costs_one_poll() -> io::Result<()> {
    let (a, mut b) = pair();
    let a = AsyncFd::new(a)?;
    let before = operations();
    let mut readable = pin!(a.readable());
    assert!(pending_once(&mut readable).await);
    assert!(pending_once(&mut readable).await);
    assert_eq!(operations() - before, 1, "one poll, polled twice");
    b.write_all(b"x")?;
    let guard = within(readable).await?;
    assert!(guard.ready().is_readable());
    assert_eq!(operations() - before, 1);
    Ok(())
}

#[ruio::test]
async fn would_block_clears_readiness_and_the_next_wait_waits() -> io::Result<()> {
    let (a, mut b) = pair();
    let a = AsyncFd::new(a)?;
    b.write_all(b"once")?;
    let mut buf = [0; 16];
    let mut guard = within(a.readable()).await?;
    assert_eq!(guard.try_io(|fd| fd.get_ref().read(&mut buf)).unwrap()?, 4);
    // Read, but not cleared: the next wait is answered at once, and the read
    // finds nothing.
    let mut guard = within(a.readable()).await?;
    let blocked = guard.try_io(|fd| fd.get_ref().read(&mut buf));
    assert!(blocked.is_err(), "nothing left to read");
    assert!(guard.ready().is_empty(), "cleared");
    drop(guard);
    let waited = ruio::time::timeout(Duration::from_millis(20), a.readable()).await;
    assert!(waited.is_err(), "cleared readiness waits again");
    b.write_all(b"again")?;
    let mut guard = within(a.readable()).await?;
    assert_eq!(guard.try_io(|fd| fd.get_ref().read(&mut buf)).unwrap()?, 5);
    Ok(())
}

#[ruio::test]
async fn clear_ready_makes_the_next_wait_poll_again() -> io::Result<()> {
    let (a, mut b) = pair();
    let a = AsyncFd::new(a)?;
    b.write_all(b"left")?;
    within(a.readable()).await?.clear_ready();
    // Cleared while the data is still there: the poll finds it at once.
    let before = operations();
    let mut readable = pin!(a.readable());
    assert!(pending_once(&mut readable).await, "polls again");
    assert_eq!(operations() - before, 1);
    let mut guard = within(readable).await?;
    assert!(guard.ready().is_readable());
    guard.clear_ready_matching(Ready::WRITABLE);
    assert!(guard.ready().is_readable(), "only what it holds is cleared");
    guard.clear_ready_matching(Ready::READABLE);
    assert!(guard.ready().is_empty());
    Ok(())
}

#[ruio::test]
async fn a_guard_dropped_without_clearing_answers_the_next_wait_at_once() -> io::Result<()> {
    let (a, mut b) = pair();
    let a = AsyncFd::new(a)?;
    b.write_all(b"kept")?;
    drop(within(a.readable()).await?);
    let before = operations();
    let mut readable = pin!(a.readable());
    assert!(!pending_once(&mut readable).await, "answered at once");
    let mut retained = within(a.readable()).await?;
    retained.retain_ready();
    drop(retained);
    let guard = a.readable().await?;
    assert!(guard.ready().is_readable());
    assert_eq!(operations(), before, "without a poll");
    Ok(())
}

#[ruio::test]
async fn writable_waits_for_room() -> io::Result<()> {
    let (a, b) = pair();
    let a = AsyncFd::new(a)?;
    let chunk = [7; 16 << 10];
    let mut written = 0;
    loop {
        let mut guard = within(a.writable()).await?;
        assert!(guard.ready().is_writable());
        match guard.try_io(|fd| fd.get_ref().write(&chunk)) {
            Ok(wrote) => written += wrote?,
            Err(_would_block) => break,
        }
    }
    assert!(written > 0);
    let waited = ruio::time::timeout(Duration::from_millis(20), a.writable()).await;
    assert!(waited.is_err(), "a full socket is not writable");
    let mut writable = pin!(a.writable());
    assert!(pending_once(&mut writable).await);
    assert_eq!(drain(&b), written);
    let guard = within(writable).await?;
    assert!(guard.ready().is_writable());
    assert!(!guard.ready().is_readable(), "only what was asked for");
    Ok(())
}

#[ruio::test]
async fn a_wait_the_cache_answers_spends_the_tasks_budget() -> io::Result<()> {
    let (a, mut b) = pair();
    let a = AsyncFd::new(a)?;
    b.write_all(b"kept")?;
    drop(within(a.readable()).await?);
    let ran = Rc::new(Cell::new(false));
    let other = ruio::spawn({
        let ran = Rc::clone(&ran);
        async move { ran.set(true) }
    });
    // Readiness kept, and answered again and again: the task yields to the
    // other once its budget is spent.
    for _ in 0..10_000 {
        drop(a.readable().await?);
        if ran.get() {
            break;
        }
    }
    assert!(ran.get(), "the other task ran");
    other.await.unwrap();
    Ok(())
}

#[ruio::test]
async fn poll_read_ready_keeps_its_wait_in_the_wrapper() -> io::Result<()> {
    let (a, mut b) = pair();
    let a = AsyncFd::new(a)?;
    let before = operations();
    let mut first = poll_fn(|cx| a.poll_read_ready(cx).map(|guard| guard.map(drop)));
    assert!(pending_once(&mut first).await);
    let mut second = poll_fn(|cx| a.poll_read_ready(cx).map(|guard| guard.map(drop)));
    assert!(pending_once(&mut second).await);
    assert_eq!(operations() - before, 1, "one poll, kept across calls");
    assert_eq!(ruio::driver().in_flight(), 1);
    b.write_all(b"polled")?;
    let mut guard = within(poll_fn(|cx| a.poll_read_ready(cx))).await?;
    let mut buf = [0; 16];
    assert_eq!(guard.try_io(|fd| fd.get_ref().read(&mut buf)).unwrap()?, 6);
    assert_eq!(&buf[..6], b"polled");
    // A write wait of its own, beside the read one.
    let guard = within(poll_fn(|cx| a.poll_write_ready(cx))).await?;
    assert!(guard.ready().is_writable());
    Ok(())
}

#[ruio::test]
async fn async_io_retries_until_the_call_succeeds() -> io::Result<()> {
    let (a, mut b) = pair();
    let a = AsyncFd::new(a)?;
    let writer = ruio::spawn(async move {
        ruio::time::sleep(Duration::from_millis(10)).await;
        b.write_all(b"later").unwrap();
        b
    });
    let mut buf = [0; 16];
    let mut calls = 0;
    let read = within(a.async_io(Interest::READABLE, |mut fd: &UnixStream| {
        calls += 1;
        fd.read(&mut buf)
    }))
    .await?;
    assert_eq!(&buf[..read], b"later");
    assert_eq!(calls, 1, "called once the wait found data");
    let _b = writer.await.unwrap();
    // Readiness left from the read: a call that blocks clears it and waits.
    let mut calls = 0;
    let waited = ruio::time::timeout(
        Duration::from_millis(20),
        a.async_io(Interest::READABLE, |mut fd: &UnixStream| {
            calls += 1;
            fd.read(&mut buf)
        }),
    )
    .await;
    assert!(waited.is_err());
    assert_eq!(calls, 1);
    Ok(())
}

#[ruio::test]
async fn a_peer_shut_down_for_writing_is_read_closed() -> io::Result<()> {
    let (a, b) = pair();
    let a = AsyncFd::new(a)?;
    let mut readable = pin!(a.readable());
    assert!(pending_once(&mut readable).await);
    // Shut down, not closed: `POLLRDHUP`, which the wait asks for, and no
    // hang-up.
    b.shutdown(std::net::Shutdown::Write)?;
    let guard = within(readable).await?;
    assert!(guard.ready().is_read_closed());
    assert!(!guard.ready().is_write_closed(), "not asked for");
    let guard = within(a.writable()).await?;
    assert!(guard.ready().is_writable());
    assert!(!guard.ready().is_write_closed(), "the write side is open");
    Ok(())
}

#[ruio::test]
async fn a_closed_peer_is_read_closed_and_write_closed() -> io::Result<()> {
    let (a, b) = pair();
    let a = AsyncFd::new(a)?;
    let mut readable = pin!(a.readable());
    assert!(pending_once(&mut readable).await);
    drop(b);
    let guard = within(readable).await?;
    assert!(guard.ready().is_read_closed());
    assert!(!guard.ready().is_write_closed(), "not asked for");
    let guard = within(a.writable()).await?;
    assert!(guard.ready().is_write_closed());
    let guard = within(a.ready(Interest::READABLE | Interest::WRITABLE)).await?;
    let ready = guard.ready();
    assert!(ready.is_read_closed() && ready.is_write_closed());
    let mut guard = within(a.readable()).await?;
    let mut buf = [0; 4];
    assert_eq!(guard.try_io(|fd| fd.get_ref().read(&mut buf)).unwrap()?, 0);
    Ok(())
}

#[ruio::test]
async fn the_mut_variants_give_the_owner_mutably() -> io::Result<()> {
    let (a, mut b) = pair();
    let mut a = AsyncFd::new(a)?;
    b.write_all(b"mutable")?;
    let mut buf = [0; 16];
    let mut guard = within(a.readable_mut()).await?;
    assert!(guard.ready().is_readable());
    let read = guard.try_io(|fd| fd.get_mut().read(&mut buf)).unwrap()?;
    assert_eq!(&buf[..read], b"mutable");
    let blocked = guard.try_io(|fd| fd.get_mut().read(&mut buf));
    assert!(blocked.is_err());
    assert!(guard.ready().is_empty());
    drop(guard);
    let waited = ruio::time::timeout(Duration::from_millis(20), a.readable_mut()).await;
    assert!(waited.is_err(), "cleared readiness waits again");

    let mut guard = within(a.writable_mut()).await?;
    assert!(guard.ready().is_writable());
    guard.get_inner_mut().write_all(b"back")?;
    assert_eq!(guard.get_inner().as_raw_fd(), guard.get_ref().as_raw_fd());
    guard.get_mut().get_mut().flush()?;
    drop(guard);
    assert_eq!(drain(&b), 4);

    b.write_all(b"polled")?;
    // The guards borrow the wrapper, which the closure has, so the polls hand
    // back their readiness rather than the guards.
    let ready = within(poll_fn(|cx| {
        a.poll_read_ready_mut(cx).map(|guard| {
            guard.map(|mut guard| {
                guard.clear_ready_matching(Ready::READ_CLOSED);
                guard.ready()
            })
        })
    }))
    .await?;
    assert!(ready.is_readable());
    let ready = within(poll_fn(|cx| {
        a.poll_write_ready_mut(cx)
            .map(|guard| guard.map(|guard| guard.ready()))
    }))
    .await?;
    assert!(ready.is_writable());

    let read = within(a.async_io_mut(Interest::READABLE, |fd| fd.read(&mut buf))).await?;
    assert_eq!(&buf[..read], b"polled");
    let guard: AsyncFdReadyMutGuard<'_, UnixStream> =
        within(a.ready_mut(Interest::WRITABLE)).await?;
    assert!(guard.ready().is_writable());
    Ok(())
}

#[ruio::test]
async fn the_wrapper_its_guards_and_its_waits_are_send() -> io::Result<()> {
    let (a, mut b) = pair();
    let a = AsyncFd::new(OwnedFd::from(a))?;
    assert_send_sync(&a);
    assert_eq!(a.as_fd().as_raw_fd(), a.get_ref().as_raw_fd());
    let readable = a.readable();
    assert_send(&readable);
    drop(readable);
    assert_send(&a.writable());
    assert_send(&a.ready(Interest::PRIORITY));
    assert_send(&a.async_io(Interest::READABLE, |_| Ok(())));
    b.write_all(b"x")?;
    let guard: AsyncFdReadyGuard<'_, OwnedFd> = within(a.readable()).await?;
    assert_send_sync(&guard);
    assert!(format!("{guard:?}").contains("is_readable: true"));
    drop(guard);
    let mut a = a;
    let guard = within(a.readable_mut()).await?;
    assert_send_sync(&guard);
    drop(guard);
    assert_send(&a.readable_mut());
    Ok(())
}

#[ruio::test]
async fn a_pending_wait_dropped_leaves_nothing_in_flight() -> io::Result<()> {
    let (a, _b) = pair();
    let a = AsyncFd::new(a)?;
    {
        let mut readable = pin!(a.readable());
        assert!(pending_once(&mut readable).await);
        assert_eq!(ruio::driver().in_flight(), 1);
    }
    ruio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(ruio::driver().in_flight(), 0, "its ring cancelled it");
    // The wait kept in the wrapper goes with it.
    let mut kept = poll_fn(|cx| a.poll_read_ready(cx).map(|guard| guard.map(drop)));
    assert!(pending_once(&mut kept).await);
    assert_eq!(ruio::driver().in_flight(), 1);
    let _a = a.into_inner();
    ruio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(ruio::driver().in_flight(), 0);
    Ok(())
}

#[test]
fn a_wait_moved_to_another_core_completes_there() {
    let outputs = two()
        .run(|core| async move {
            if core.index() != 0 {
                return None;
            }
            let (a, mut b) = pair();
            let a = AsyncFd::new(a).unwrap();
            let mut wait = Box::pin(async move {
                let mut guard = a.readable().await?;
                let mut buf = [0; 16];
                let read = guard.try_io(|fd| fd.get_ref().read(&mut buf)).unwrap()?;
                io::Result::Ok((buf[..read].to_vec(), ruio::core().unwrap().index()))
            });
            assert!(pending_once(&mut wait).await, "queued here");
            let away = ruio::spawn_on(1, wait);
            b.write_all(b"moved").unwrap();
            Some(within(away).await.unwrap())
        })
        .unwrap();
    let (data, ran_on) = outputs.into_iter().next().flatten().unwrap().unwrap();
    assert_eq!((&data[..], ran_on), (&b"moved"[..], 1));
}
