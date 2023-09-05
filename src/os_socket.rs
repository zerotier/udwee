#[allow(unused_imports)]
use num_traits::AsPrimitive;

use zerotier_common_utils::inetaddress::InetAddress;

#[allow(unused)]
#[cfg(unix)]
pub(crate) unsafe fn bind_udp(
    address: &InetAddress,
    bind_to_device: Option<&str>,
    reuseport: bool,
    v6only: bool,
    nonblock: bool,
) -> Result<i32, &'static str> {
    use libc::*;
    use zerotier_common_utils::inetaddress::InetAddress;

    let mut setsockopt_results: c_int = 0;
    let mut fl;

    let (af, sa_len) = if address.is_ipv4() {
        (AF_INET, std::mem::size_of::<sockaddr_in>().as_())
    } else if address.is_ipv6() {
        (AF_INET6, std::mem::size_of::<sockaddr_in6>().as_())
    } else {
        return Err("unrecognized address family");
    };

    let s = socket(af.as_(), SOCK_DGRAM, 0);
    if s <= 0 {
        return Err("unable to create new UDP socket");
    }

    if nonblock {
        fcntl(s, F_SETFL, O_NONBLOCK);
    }

    /*
    let mut timeo: timeval = std::mem::zeroed();
    timeo.tv_sec = SOCKET_RECV_TIMEOUT_SECONDS.as_();
    timeo.tv_usec = 0;
    setsockopt_results |= setsockopt(
        s,
        SOL_SOCKET.as_(),
        SO_RCVTIMEO.as_(),
        (&mut timeo as *mut timeval).cast(),
        std::mem::size_of::<timeval>().as_(),
    );
    */

    if reuseport {
        fl = 1;
        if setsockopt(
            s,
            SOL_SOCKET.as_(),
            SO_REUSEPORT.as_(),
            (&mut fl as *mut c_int).cast(),
            std::mem::size_of::<c_int>().as_(),
        ) != 0
        {
            close(s);
            return Err("unable to set SO_REUSEPORT");
        }
    }

    fl = 1;
    setsockopt(
        s,
        SOL_SOCKET.as_(),
        SO_BROADCAST.as_(),
        (&mut fl as *mut c_int).cast(),
        std::mem::size_of::<c_int>().as_(),
    );

    if af == AF_INET6 {
        fl = if v6only {
            1
        } else {
            0
        };
        if setsockopt(
            s,
            IPPROTO_IPV6.as_(),
            IPV6_V6ONLY.as_(),
            (&mut fl as *mut c_int).cast(),
            std::mem::size_of::<c_int>().as_(),
        ) != 0
        {
            close(s);
            return Err("unable to set V6ONLY");
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Some(bind_to_device) = bind_to_device {
            if !bind_to_device.is_empty() {
                let _ = std::ffi::CString::new(bind_to_device).map(|dn| {
                    let dnb = dn.as_bytes_with_nul();
                    let _ = setsockopt(
                        s.as_(),
                        SOL_SOCKET.as_(),
                        SO_BINDTODEVICE.as_(),
                        dnb.as_ptr().cast(),
                        (dnb.len() - 1).as_(),
                    );
                });
            }
        }
    }

    if setsockopt_results != 0 {
        close(s);
        return Err("setsockopt() failed");
    }

    if af == AF_INET {
        #[cfg(not(target_os = "linux"))]
        {
            fl = 0;
            setsockopt(
                s,
                IPPROTO_IP.as_(),
                IP_DONTFRAG.as_(),
                (&mut fl as *mut c_int).cast(),
                std::mem::size_of::<c_int>().as_(),
            );
        }
        #[cfg(target_os = "linux")]
        {
            fl = IP_PMTUDISC_DONT as c_int;
            setsockopt(
                s,
                IPPROTO_IP.as_(),
                IP_MTU_DISCOVER.as_(),
                (&mut fl as *mut c_int).cast(),
                std::mem::size_of::<c_int>().as_(),
            );
        }
    }

    if af == AF_INET6 {
        fl = 0;
        setsockopt(
            s,
            IPPROTO_IPV6.as_(),
            IPV6_DONTFRAG.as_(),
            (&mut fl as *mut c_int).cast(),
            std::mem::size_of::<c_int>().as_(),
        );
    }

    fl = 1048576;
    while fl >= 65536 {
        if setsockopt(
            s,
            SOL_SOCKET.as_(),
            SO_RCVBUF.as_(),
            (&mut fl as *mut c_int).cast(),
            std::mem::size_of::<c_int>().as_(),
        ) == 0
        {
            break;
        }
        fl -= 65536;
    }
    fl = 1048576;
    while fl >= 65536 {
        if setsockopt(
            s,
            SOL_SOCKET.as_(),
            SO_SNDBUF.as_(),
            (&mut fl as *mut c_int).cast(),
            std::mem::size_of::<c_int>().as_(),
        ) == 0
        {
            break;
        }
        fl -= 65536;
    }

    if bind(s, (address as *const InetAddress).cast(), sa_len) != 0 {
        close(s);
        return Err("bind to address failed");
    }

    Ok(s as i32)
}
