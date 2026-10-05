#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = fighter_protocol::text::clean_name(text);
        let _ = fighter_protocol::text::clean_room_name(text, "Host");
    }
});
