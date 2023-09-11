/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * (c) ZeroTier, Inc.
 * https://www.zerotier.com/
 */

mod os_socket;

use std::collections::HashSet;
use std::error::Error;
use std::mem::size_of;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

#[allow(unused_imports)]
use num_traits::AsPrimitive;

use polling::{Event, Poller};
use zerotier_common_utils::inetaddress::InetAddress;

/// Trait to be implemented to handle packets read from UDP sockets.
pub trait PacketHandler: Send + Sync + Sized + 'static {
    /// Data type for buffers passed in and out of the UDP I/O engine.
    ///
    /// This is commonly Box<[u8]> or similar. It should be a thin pointer and not a whole literal
    /// buffer to avoid a lot of memory copying.
    type Buffer: AsMut<[u8]> + Send;

    /// A cloneable reference to the handler, typically Arc<...>.
    type Ref: AsRef<Self> + Send + Clone;

    /// Data type for arbitrary data to attach to a socket.
    ///
    /// It must be safe an efficient to clone this since a copy will be distributed to each
    /// worker thread. This would typically be a small bit of static data or an Arc<> or Weak<>.
    ///
    /// Use () if this is not needed.
    type SocketApplicationData: Send + Clone;

    /// Obtain a buffer to receive a new packet.
    ///
    /// Receive buffers must be sized to accept the largest possible UDP packet or packets may
    /// be lost. In other words if the buffer is e.g. a Vec<u8> its size must be set with
    /// resize() prior to returning from this so that as_mut() will return a destination buffer
    /// for reading. The buffer does not need to be cleared as any data will be overwritten.
    fn get_receive_buffer(&self) -> Self::Buffer;

    /// Called by the engine to return a buffer after a send completes (or fails).
    ///
    /// This isn't called after on_udp_packet() since that handler takes ownership of the buffer.
    /// If you are doing pooling you could instead call this internally inside on_udp_packet when
    /// processing is complete.
    ///
    /// The default implementation just drops. Override if anything needs to be done with used
    /// buffers other than dropping them.
    #[inline(always)]
    fn return_buffer(&self, buffer: Self::Buffer) {
        drop(buffer);
    }

    /// Called whan a UDP packet is received.
    ///
    /// This is called directly (and concurrently) from the actual I/O thread(s) internal
    /// to the engine. It therefore should never do anything time consuming unless you are
    /// okay with blocking engine threads. Queues or channels can be used to send packets
    /// elsewhere for processing. This also works for interoperability with async code.
    fn on_udp_packet(&self, socket: &UdpSocket<Self>, remote_address: &InetAddress, data: Self::Buffer, len: usize);
}

/// Fast UDP I/O engine.
pub struct Engine<H: PacketHandler> {
    threads: Vec<Arc<EngineThread<H>>>,
}

/// Global set of bound local addresses since with SO_REUSEPORT we can't use failure to bind to
/// determine if an address is already in use by our own process.
///
/// It needs to be global in case multiple engine instances are created.
///
/// This is populated on bind(). Addresses are removed on close() or within threads during
/// engine shutdown.
static BOUND_LOCAL_ADDRESSES: Mutex<Option<HashSet<InetAddress>>> = Mutex::new(None);

/// Bound UDP socket handle.
///
/// UdpSocket doesn't implement Clone because the socket returned from bind() is meant to
/// uniquely represent the socket in the application. Shadow socket instances are also created
/// internally to supply each thread with one to give to on_udp_packet() to send packets within
/// a receive handler, but these are not "canonical" and cloning them would create confusion.
///
/// Wrapping the returned socket in Rc<> or Arc<> is fine if you want auto-GC inside your
/// application.
pub struct UdpSocket<H: PacketHandler> {
    fd: RawFd,

    /// Unique internal ID (not a file descriptor) for this socket.
    pub id: usize,
    /// Local address to which this socket is bound.
    pub local_address: InetAddress,
    /// Handler supplied when opening this socket.
    pub handler: H::Ref,
    /// Arbitrary application data attached to socket.
    pub data: H::SocketApplicationData,
}

/// Commands that can be sent to worker threads.
enum ThreadCommand<H: PacketHandler> {
    Open(UdpSocket<H>),
    Close(usize),
    Shutdown,
}

/// State information for each worker thread.
struct EngineThread<H: PacketHandler> {
    poller: Poller,
    commands: Mutex<Vec<ThreadCommand<H>>>,
    thread: JoinHandle<()>,
}

impl<H: PacketHandler> Engine<H> {
    /// Create a new engine.
    pub fn new() -> Self {
        let wait_for_arc_init = |self_ref: Weak<EngineThread<H>>| loop {
            if let Some(self_ref) = self_ref.upgrade() {
                self_ref.thread_main();
                break;
            } else {
                // wait for self_ref to be fully constructed by Arc::new_cyclic()
                std::thread::sleep(Duration::from_millis(1));
            }
        };
        Self {
            threads: if let Some(core_ids) = core_affinity::get_core_ids() {
                assert!(core_ids.len() > 0);
                core_ids
                    .into_iter()
                    .map(|core_id| {
                        Arc::new_cyclic(|self_ref: &Weak<EngineThread<H>>| {
                            let self_ref = self_ref.clone();
                            EngineThread {
                                poller: Poller::new().unwrap(),
                                commands: Mutex::new(Vec::with_capacity(8)),
                                thread: std::thread::spawn(move || {
                                    core_affinity::set_for_current(core_id);
                                    wait_for_arc_init(self_ref);
                                }),
                            }
                        })
                    })
                    .collect()
            } else {
                let hw_par = std::thread::available_parallelism().unwrap().get();
                let mut threads = Vec::with_capacity(hw_par);
                for _ in 0..hw_par {
                    threads.push(Arc::new_cyclic(|self_ref: &Weak<EngineThread<H>>| {
                        let self_ref = self_ref.clone();
                        EngineThread {
                            poller: Poller::new().unwrap(),
                            commands: Mutex::new(Vec::with_capacity(8)),
                            thread: std::thread::spawn(move || wait_for_arc_init(self_ref)),
                        }
                    }))
                }
                threads
            },
        }
    }

    /// Bind a UDP socket to a local address.
    ///
    /// The returned UdpSocket must be closed explicitly with close() when the application is
    /// finished with it.
    ///
    /// Packets are sent via the returned socket object. When packets are received the handler
    /// will be called directly.
    ///
    /// * `bind_address`: Local IP address and port
    /// * `bind_to_device`: If specified, bind to a network interface (platform-specific, mainly Linux)
    /// * `v6only`: If true IPv6 sockets bound to ::0 should only receive IPv6 datagrams.
    /// * `handler`: Reference to handler instance for packets read from this socket.
    /// * `data`: Arbitrary data to attach to socket.
    pub fn bind(
        &self,
        bind_address: &InetAddress,
        bind_to_device: Option<&str>,
        v6only: bool,
        handler: H::Ref,
        data: H::SocketApplicationData,
    ) -> Result<UdpSocket<H>, Box<dyn Error>> {
        // Counter used to assign each UdpSocket an internally unique ID.
        static ID_COUNTER: AtomicUsize = AtomicUsize::new(1);

        let mut bound = BOUND_LOCAL_ADDRESSES.lock().unwrap(); // also serializes calls to bind()

        if bound.as_ref().map_or(false, |b| b.contains(bind_address)) {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                "already bound to this address",
            )));
        }

        let mut fds = Vec::with_capacity(self.threads.len());
        for _ in 0..self.threads.len() {
            match unsafe { crate::os_socket::bind_udp(bind_address, bind_to_device, true, v6only, true) } {
                Ok(fd) => fds.push(fd),
                Err(desc) => {
                    for fd in fds.iter() {
                        unsafe { libc::close(*fd) };
                    }
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::AddrNotAvailable,
                        desc,
                    )));
                }
            }
        }

        let id = ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        bound.get_or_insert_with(|| HashSet::new()).insert(bind_address.clone());

        for (t, fd) in self.threads.iter().zip(fds.iter()) {
            t.commands.lock().unwrap().push(ThreadCommand::Open(UdpSocket {
                fd: *fd,
                id,
                local_address: bind_address.clone(),
                handler: handler.clone(),
                data: data.clone(),
            }));
            let _ = t.poller.notify();
        }

        Ok(UdpSocket {
            fd: *fds.first().unwrap(),
            id,
            local_address: bind_address.clone(),
            handler,
            data,
        })
    }

    /// Close a socket.
    ///
    /// Note that it's possible for a few packets to continue to be received on this socket
    /// until all threads have had a chance to receive a close command for it.
    pub fn close(&self, socket: UdpSocket<H>) {
        if BOUND_LOCAL_ADDRESSES
            .lock()
            .unwrap()
            .as_mut()
            .map_or(false, |b| b.remove(&socket.local_address))
        {
            for t in self.threads.iter() {
                t.commands.lock().unwrap().push(ThreadCommand::Close(socket.id));
                let _ = t.poller.notify();
            }
        }
    }
}

impl<H: PacketHandler> Drop for Engine<H> {
    fn drop(&mut self) {
        for t in self.threads.drain(..) {
            t.commands.lock().unwrap().insert(0, ThreadCommand::Shutdown); // tell thread to exit
            loop {
                let _ = t.poller.notify();
                if t.thread.is_finished() {
                    break;
                } else {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
    }
}

impl<H: PacketHandler> EngineThread<H> {
    fn thread_main(&self) {
        let mut events = Vec::with_capacity(16);
        let mut commands = Vec::with_capacity(16);
        let mut sockets: Vec<Pin<Box<UdpSocket<H>>>> = Vec::with_capacity(16);
        let mut from_address = InetAddress::new();
        loop {
            if self.poller.wait(&mut events, None).is_err() {
                panic!("polling failed");
            }

            for ev in events.iter() {
                match ev {
                    Event { key, readable, writable: _writable } => {
                        if *readable {
                            let d: &UdpSocket<H> = unsafe { &*(*key as *const UdpSocket<H>) };
                            let handler = d.handler.as_ref();
                            loop {
                                let mut buf = handler.get_receive_buffer();
                                let buf_inner = buf.as_mut();
                                let mut addrlen = size_of::<InetAddress>() as libc::socklen_t;
                                let packet_size = unsafe {
                                    libc::recvfrom(
                                        d.fd,
                                        buf_inner.as_mut_ptr().cast(),
                                        buf_inner.len().as_(),
                                        0,
                                        (&mut from_address as *mut InetAddress).cast(),
                                        (&mut addrlen as *mut libc::socklen_t).cast(),
                                    ) as isize
                                };
                                if packet_size >= 0 {
                                    handler.on_udp_packet(d, &from_address, buf, packet_size as usize);
                                } else {
                                    break;
                                }
                            }
                            let _ = self
                                .poller
                                .modify(d.fd, Event { key: *key, readable: true, writable: false });
                        }
                    }
                }
            }
            events.clear();

            std::mem::swap(self.commands.lock().unwrap().as_mut(), &mut commands);
            for command in commands.drain(..) {
                match command {
                    ThreadCommand::Open(socket) => {
                        let fd = socket.fd;
                        sockets.push(Box::pin(socket));
                        self.poller
                            .add(
                                fd,
                                Event {
                                    key: (&*sockets.last().unwrap().as_ref() as *const UdpSocket<H>) as usize,
                                    readable: true,
                                    writable: false,
                                },
                            )
                            .unwrap();
                    }
                    ThreadCommand::Close(id) => sockets.retain(|d| {
                        if d.id == id {
                            self.poller.delete(d.fd).unwrap();
                            unsafe { libc::close(d.fd) };
                            false
                        } else {
                            true
                        }
                    }),
                    ThreadCommand::Shutdown => {
                        let mut bound = BOUND_LOCAL_ADDRESSES.lock().unwrap();
                        for d in sockets.iter() {
                            bound.as_mut().map(|b| b.remove(&d.local_address));
                            unsafe { libc::close(d.fd) };
                        }
                        return;
                    }
                }
            }
        }
    }
}

impl<H: PacketHandler> UdpSocket<H> {
    /// Send a packet over this socket.
    ///
    /// This doesn't provide a return value because (1) UDP does not guarantee delivery anyway
    /// and (2) on some platforms a queue might be used to batch sends and so no feedback will
    /// be immediately available.
    ///
    /// Attempting to send on a socket after the engine that created it has been dropped will
    /// either silently fail or panic depending on the implementation.
    #[inline]
    pub fn send(&self, dest: &InetAddress, mut data: H::Buffer, len: usize) {
        // Basic implementation for most platforms.
        unsafe {
            libc::sendto(
                self.fd,
                data.as_mut().as_ptr().cast(),
                len.as_(),
                0,
                (dest as *const InetAddress).cast(),
                size_of::<InetAddress>().as_(),
            );
            self.handler.as_ref().return_buffer(data);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use zerotier_common_utils::buf::Buf;

    #[derive(Default)]
    struct TestHandler {
        received: AtomicU64,
        echo: bool,
    }

    impl PacketHandler for TestHandler {
        type Buffer = Buf;
        type Ref = Arc<Self>;
        type SocketApplicationData = ();

        fn get_receive_buffer(&self) -> Self::Buffer {
            let mut b = Buf::new(1504);
            unsafe { b.set_size(1500) };
            b
        }

        fn on_udp_packet(
            &self,
            socket: &UdpSocket<Self>,
            remote_address: &InetAddress,
            data: Self::Buffer,
            len: usize,
        ) {
            self.received.fetch_add(len as u64, Ordering::SeqCst);
            if self.echo {
                socket.send(remote_address, data, len);
                //println!("echo! {} {}", len, self.received.load(Ordering::Relaxed));
            } else {
                self.return_buffer(data);
            }
        }
    }

    #[test]
    fn loopback() {
        let eng = Engine::<TestHandler>::new();

        let sender_handler = Arc::new(TestHandler { received: AtomicU64::new(0), echo: false });
        let sender = eng
            .bind(
                &InetAddress::from_ip_port(&[127, 0, 0, 1], 11111),
                None,
                true,
                sender_handler.clone(),
                (),
            )
            .unwrap();

        let receiver_handler = Arc::new(TestHandler { received: AtomicU64::new(0), echo: true });
        let sendto_addr = InetAddress::from_ip_port(&[127, 0, 0, 1], 11112);
        let _receiver = eng
            .bind(&sendto_addr, None, true, receiver_handler.clone(), ())
            .unwrap();

        const PACKET_COUNT: usize = 1024;
        for _ in 0..PACKET_COUNT {
            let mut b = sender_handler.get_receive_buffer();
            b.as_mut().fill(1);
            sender.send(&sendto_addr, b, 1024);
        }

        for _ in 0..5000 {
            if sender_handler.received.load(Ordering::Relaxed) == (PACKET_COUNT * 1024) as u64
                && receiver_handler.received.load(Ordering::Relaxed) == (PACKET_COUNT * 1024) as u64
            {
                return;
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        panic!("receive timed out");
    }
}
