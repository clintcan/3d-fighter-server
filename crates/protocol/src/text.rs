//! Name and room-name cleaning, section 10.4.

/// Maximum display-name length in Unicode scalar values.
pub const MAX_NAME_CHARS: usize = 24;
/// Maximum room-name length in Unicode scalar values.
pub const MAX_ROOM_NAME_CHARS: usize = 32;

/// A character the specification says to strip from all names and room names.
fn is_forbidden(c: char) -> bool {
    let u = c as u32;
    u < 0x0020
        || (0x007F..=0x009F).contains(&u)
        || (0x200B..=0x200F).contains(&u)
        || (0x2028..=0x202E).contains(&u)
        || (0x2060..=0x206F).contains(&u)
        || u == 0xFEFF
}

/// Remove forbidden characters and trim. Length is not capped here.
pub fn clean_text(input: &str) -> String {
    input
        .chars()
        .filter(|&c| !is_forbidden(c))
        .collect::<String>()
        .trim()
        .to_string()
}

fn truncate_chars(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

/// Clean a player name: strip, trim, cap at [`MAX_NAME_CHARS`], fall back to
/// `"Player"` when nothing is left.
pub fn clean_name(raw: &str) -> String {
    let cleaned = truncate_chars(&clean_text(raw), MAX_NAME_CHARS);
    if cleaned.is_empty() {
        "Player".to_string()
    } else {
        cleaned
    }
}

/// Clean a room name, falling back to `"<host>'s room"` when empty. `host_name`
/// should already be cleaned with [`clean_name`].
pub fn clean_room_name(raw: &str, host_name: &str) -> String {
    let cleaned = truncate_chars(&clean_text(raw), MAX_ROOM_NAME_CHARS);
    if cleaned.is_empty() {
        format!("{host_name}'s room")
    } else {
        cleaned
    }
}

/// Case-insensitive blocklist check against an already-cleaned name.
pub fn is_blocked(name: &str, blocklist: &[String]) -> bool {
    let lower = name.to_lowercase();
    blocklist.iter().any(|bad| {
        let bad = bad.to_lowercase();
        !bad.is_empty() && lower.contains(&bad)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_control_and_bidi() {
        let dirty = "Li\u{0000}no\u{202E}";
        assert_eq!(clean_name(dirty), "Lino");
    }

    #[test]
    fn strips_zero_width_and_bom() {
        assert_eq!(clean_name("\u{200B}\u{FEFF}Lufi\u{200F}"), "Lufi");
    }

    #[test]
    fn strips_line_and_paragraph_separators() {
        assert_eq!(clean_name("a\u{2028}b\u{2029}c"), "abc");
    }

    #[test]
    fn trims_and_caps_names() {
        assert_eq!(clean_name("   spaced   "), "spaced");
        let long = "x".repeat(100);
        assert_eq!(clean_name(&long).chars().count(), MAX_NAME_CHARS);
    }

    #[test]
    fn empty_name_falls_back() {
        assert_eq!(clean_name("   "), "Player");
        assert_eq!(clean_name("\u{202E}\u{200B}"), "Player");
    }

    #[test]
    fn empty_room_name_uses_host() {
        assert_eq!(clean_room_name("", "Lino"), "Lino's room");
        assert_eq!(clean_room_name("   ", "Lino"), "Lino's room");
        assert_eq!(clean_room_name("Ranked", "Lino"), "Ranked");
    }

    #[test]
    fn blocklist_is_case_insensitive_and_substring() {
        let list = vec!["slur".to_string()];
        assert!(is_blocked("a SLUR here", &list));
        assert!(!is_blocked("clean", &list));
    }
}
