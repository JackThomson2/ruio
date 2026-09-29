//! Direct descriptors: files in a slot of a ring's registered file table.

mod support;

use std::io::{self, Write as _};
use std::os::fd::AsRawFd;

use ruio_driver::buf::BufPool;
use ruio_driver::ops::{Read, ReadPooled, Socket};
use ruio_driver::{Descriptor, Driver, Fd};

use support::{block_on, poll_once, settle};

fn direct_socket(driver: &Driver) -> io::Result<Descriptor> {
    let socket = Socket::new(libc::AF_INET, libc::SOCK_STREAM, 0).direct(driver);
    block_on(driver, driver.op(socket))
}

fn slot(descriptor: &Descriptor) -> u32 {
    match descriptor.fd() {
        Fd::Direct(slot) => slot,
        fd => panic!("{fd:?} is not a direct descriptor"),
    }
}

#[test]
fn rings_get_a_file_table_by_default() {
    let driver = Driver::new().unwrap();
    assert!((1..=4096).contains(&driver.files()), "{}", driver.files());
    assert!(direct_socket(&driver).unwrap().is_direct());
}

#[test]
fn direct_sockets_take_slots_until_the_table_is_full() {
    let driver = Driver::builder().files(3).build().unwrap();
    let mut sockets: Vec<Descriptor> = (0..3).map(|_| direct_socket(&driver).unwrap()).collect();
    let mut slots: Vec<u32> = sockets.iter().map(slot).collect();
    slots.sort_unstable();
    assert_eq!(slots, [0, 1, 2]);
    let full = direct_socket(&driver).unwrap_err();
    assert_eq!(full.raw_os_error(), Some(libc::ENFILE));

    // Dropping a socket frees its slot, even for the very next request.
    let freed = slot(&sockets.remove(1));
    let socket = direct_socket(&driver).unwrap();
    assert_eq!(slot(&socket), freed);
}

/// The socket sits in the slot whose number is the descriptor of a pipe
/// holding data, so an operation that took the slot for a descriptor would
/// read the pipe instead.
#[test]
fn operations_reach_a_direct_socket_through_the_table() {
    let driver = Driver::builder().files(256).build().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    writer.write_all(b"pipe data").unwrap();
    let target = u32::try_from(reader.as_raw_fd()).unwrap();
    let sockets: Vec<Descriptor> = (0..=target)
        .map(|_| direct_socket(&driver).unwrap())
        .collect();
    let socket = sockets.last().unwrap().fd();
    assert_eq!(socket, Fd::Direct(target));

    // An unconnected stream socket has nothing to read.
    let (read, _) = block_on(
        &driver,
        driver.op(Read::new(socket, Vec::with_capacity(16))),
    );
    assert_eq!(read.unwrap_err().raw_os_error(), Some(libc::ENOTCONN));
    let pool = BufPool::new(&driver, 2, 16).unwrap();
    let read = block_on(&driver, driver.op(ReadPooled::new(socket, &pool)));
    assert_eq!(read.unwrap_err().raw_os_error(), Some(libc::ENOTCONN));
    assert_eq!(pool.available(), 2);
}

#[test]
fn only_a_file_table_asked_for_must_exist() {
    // More slots than the kernel ever allows.
    let err = Driver::builder().files(1 << 21).build().unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EMFILE));

    let driver = Driver::builder().files(0).build().unwrap();
    assert_eq!(driver.files(), 0);
    assert!(direct_socket(&driver).is_err());
    let socket = Socket::new(libc::AF_INET, libc::SOCK_STREAM, 0);
    let plain = block_on(&driver, driver.op(socket)).unwrap();
    assert!(matches!(plain, Descriptor::Plain(_)));
}

/// However far a request got before its future was dropped, the socket it
/// made is closed and its slot freed.
#[test]
fn sockets_of_dropped_requests_are_closed() {
    let driver = Driver::builder().files(2).build().unwrap();
    for polls in 0..4 {
        let mut op = driver.op(Socket::new(libc::AF_INET, libc::SOCK_STREAM, 0).direct(&driver));
        for _ in 0..polls {
            if poll_once(&mut op).is_ready() {
                break;
            }
            driver.submit().unwrap();
        }
        drop(op);
        settle(&driver);
    }
    let sockets: io::Result<Vec<_>> = (0..2).map(|_| direct_socket(&driver)).collect();
    assert_eq!(sockets.unwrap().len(), 2);
}

#[test]
fn a_direct_descriptor_names_its_ring_without_keeping_it_open() {
    let driver = Driver::new().unwrap();
    let Descriptor::Direct(socket) = direct_socket(&driver).unwrap() else {
        panic!("not a direct descriptor");
    };
    assert_eq!(socket.driver().map(|ring| ring.id()), Some(driver.id()));
    let other = Driver::new().unwrap();
    assert_ne!(other.id(), driver.id());

    drop(driver);
    assert!(socket.driver().is_none());
    drop(socket);
}
