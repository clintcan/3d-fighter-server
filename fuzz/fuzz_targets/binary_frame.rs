#![no_main]

use fighter_protocol::feed::FeedFrame;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(frame) = FeedFrame::decode(data) {
        let _ = frame.encode();
    }
});
