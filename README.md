UDWee
======

UDWee makes UDP go *weeeeeeeeee* as in really fast. It uses the following approaches on each platform:

 * macOS / BSD: SO_REUSEPORT and thread-per-core I/O.
 * Linux: SO_REUSEPORT and thread-per-core with `recvmmsg` and `sendmmsg` (and io_uring possibly in the future)
 * Windows: TBD

It's designed for use with ZeroTier but can be used by other projects as well. The `InetAddress` class from the ZeroTier common utilities crate is used instead of the usual `SocketAddr` as it can easily be cast to/from the low-level OS `sockaddr` structures, but it implements into/from traits for `SocketAddr` and friends.
