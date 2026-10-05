#![no_main]

use fighter_protocol::udp::UdpDatagram;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(datagram) = UdpDatagram::decode(data) {
        let _ = datagram.encode();
    }
});
