//! Direct descriptors sent from one ring's file table to another's.

mod support;

use std::io::{self, Read as _};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::mpsc;
use std::thread;

use ruio_driver::ops::{Accept, Send, Socket};
use ruio_driver::{Descriptor, DirectFd, Driver, SentFd};

use support::{PATIENCE, block_on};

fn direct_socket(driver: &Driver) -> io::Result<DirectFd> {
    let socket = Socket::new(libc::AF_INET, libc::SOCK_STREAM, 0).direct(driver);
    match block_on(driver, driver.op(socket))? {
        Descriptor::Direct(fd) => Ok(fd),
        Descriptor::Plain(_) => panic!("not a direct descriptor"),
    }
}

/// A connection accepted on one ring moves to another's, on another thread,
/// which writes to it; the first ring's slot is free again.
#[test]
fn a_direct_descriptor_moves_to_a_ring_on_another_thread() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    client.set_read_timeout(Some(PATIENCE)).unwrap();
    let driver = Driver::builder().files(1).build().unwrap();
    let accept = Accept::new(listener.as_raw_fd()).direct(&driver);
    let (Descriptor::Direct(accepted), _) = block_on(&driver, driver.op(accept)).unwrap() else {
        panic!("not a direct descriptor");
    };

    let (rings, ring) = mpsc::channel();
    let (sends, sent) = mpsc::channel::<SentFd>();
    let other = thread::spawn(move || {
        let driver = Driver::new().unwrap();
        rings.send(driver.remote()).unwrap();
        let fd = sent
            .recv()
            .unwrap()
            .claim()
            .expect("claimed on its ring's thread");
        assert!(fd.belongs_to(&driver));
        let send = Send::new(fd.fd(), b"moved".to_vec());
        let (written, _) = block_on(&driver, driver.op(send));
        assert_eq!(written.unwrap(), 5);
    });
    let to = ring.recv().unwrap();
    let sent = block_on(&driver, accepted.send_to(&to)).unwrap();
    assert_eq!(sent.ring(), to.id());
    // This ring let go of the connection, and has its one slot free.
    assert_eq!(direct_socket(&driver).unwrap().slot(), 0);
    sends.send(sent).unwrap();
    other.join().unwrap();
    let mut received = [0; 5];
    client.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"moved");
}

/// A descriptor comes back, still open, when the other ring cannot take it:
/// its table is full, it is the same ring, or it is closed.
#[test]
fn a_descriptor_the_other_ring_cannot_take_comes_back() {
    let driver = Driver::new().unwrap();
    let full = Driver::builder().files(1).build().unwrap();
    let filler = direct_socket(&full).unwrap();
    let fd = direct_socket(&driver).unwrap();
    let slot = fd.slot();

    let (err, fd) = block_on(&driver, fd.send_to(&full.remote())).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENFILE));
    let (err, fd) = block_on(&driver, fd.send_to(&driver.remote())).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
    // A ring opened after the closed one likely takes its descriptor's
    // number, and must not get the descriptor meant for the closed one.
    let closed = Driver::new().unwrap().remote();
    let _reusing = Driver::new().unwrap();
    let (err, fd) = block_on(&driver, fd.send_to(&closed)).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EBADF));
    assert_eq!(fd.slot(), slot);

    // Once the full table has room, the descriptor moves there.
    drop(filler);
    full.submit().unwrap();
    let sent = block_on(&driver, fd.send_to(&full.remote())).unwrap();
    let fd = sent.claim().unwrap();
    assert!(fd.belongs_to(&full));
    assert_eq!(fd.slot(), 0);
}

/// A descriptor sent and never claimed is closed on its ring, whichever
/// thread drops it.
#[test]
fn an_unclaimed_descriptor_is_closed_on_its_ring() {
    let driver = Driver::new().unwrap();
    let to = Driver::builder().files(1).build().unwrap();
    for elsewhere in [false, true] {
        let fd = direct_socket(&driver).unwrap();
        let sent = block_on(&driver, fd.send_to(&to.remote())).unwrap();
        assert_eq!(sent.slot(), 0);
        if elsewhere {
            thread::spawn(move || drop(sent)).join().unwrap();
        } else {
            drop(sent);
        }
        // The close reaches the kernel at the ring's next turn, before the
        // socket asks for the slot.
        to.submit().unwrap();
        let socket = direct_socket(&to).unwrap();
        assert_eq!(socket.slot(), 0, "dropped elsewhere: {elsewhere}");
        // Free again, for the next round.
        drop(socket);
        to.submit().unwrap();
    }
}

/// Only its ring's thread can claim a descriptor: anywhere else the token
/// comes back.
#[test]
fn a_descriptor_is_claimed_on_its_ring_thread_only() {
    let driver = Driver::new().unwrap();
    let to = Driver::new().unwrap();
    let fd = direct_socket(&driver).unwrap();
    let sent = block_on(&driver, fd.send_to(&to.remote())).unwrap();
    let sent = thread::spawn(move || sent.claim().unwrap_err())
        .join()
        .unwrap();
    let fd = sent.claim().unwrap();
    assert!(fd.belongs_to(&to));
}
