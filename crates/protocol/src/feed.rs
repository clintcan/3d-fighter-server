//! Spectator feed frames (section 8.1). Little-endian, identical in both
//! directions. The server validates host frames with these decoders.

use crate::error::{le, CodecError};

pub const TYPE_MATCH_START: u8 = 0x01;
pub const TYPE_INPUTS: u8 = 0x02;
pub const TYPE_CHECKSUM: u8 = 0x03;
pub const TYPE_MATCH_END: u8 = 0x04;
pub const TYPE_FEED_RESET: u8 = 0x05;

/// Current feed version in MATCH_START.
pub const FEED_VERSION: u16 = 1;
/// Maximum bytes in each MATCH_START string.
pub const MAX_STRING_BYTES: usize = 64;
/// Maximum ticks in one INPUTS frame.
pub const MAX_INPUTS_COUNT: u16 = 600;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchResult {
    P1Won,
    P2Won,
    Draw,
    Aborted,
}

impl MatchResult {
    fn to_byte(self) -> u8 {
        match self {
            MatchResult::P1Won => 0,
            MatchResult::P2Won => 1,
            MatchResult::Draw => 2,
            MatchResult::Aborted => 3,
        }
    }

    fn from_byte(b: u8) -> Result<Self, CodecError> {
        match b {
            0 => Ok(MatchResult::P1Won),
            1 => Ok(MatchResult::P2Won),
            2 => Ok(MatchResult::Draw),
            3 => Ok(MatchResult::Aborted),
            _ => Err(CodecError::BadValue("match result")),
        }
    }
}

/// True when a per-tick input word follows section 2: only bits 0-8 set and a
/// direction of 1-9 in bits 0-3.
pub fn validate_input(input: u16) -> bool {
    let dir = input & 0x000F;
    (1..=9).contains(&dir) && (input & 0xFE00) == 0
}

fn put_str(out: &mut Vec<u8>, s: &str) -> Result<(), CodecError> {
    let bytes = s.as_bytes();
    if bytes.len() > MAX_STRING_BYTES {
        return Err(CodecError::BadValue("string too long"));
    }
    out.push(bytes.len() as u8);
    out.extend_from_slice(bytes);
    Ok(())
}

fn get_str(buf: &[u8], pos: &mut usize) -> Result<String, CodecError> {
    let len = le::get_u8(buf, pos)? as usize;
    if len > MAX_STRING_BYTES {
        return Err(CodecError::BadValue("string too long"));
    }
    let end = pos.checked_add(len).ok_or(CodecError::Truncated)?;
    let slice = buf.get(*pos..end).ok_or(CodecError::Truncated)?;
    *pos = end;
    let s = std::str::from_utf8(slice).map_err(|_| CodecError::Utf8)?;
    Ok(s.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchStart {
    pub feed_version: u16,
    pub match_id: u32,
    pub stage_index: u8,
    pub p1_fighter_index: u8,
    pub p2_fighter_index: u8,
    pub game_version: String,
    pub stage_id: String,
    pub p1_fighter_id: String,
    pub p2_fighter_id: String,
    pub p1_name: String,
    pub p2_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchEnd {
    pub match_id: u32,
    pub final_tick: u32,
    pub result: MatchResult,
    pub p1_wins: u8,
    pub p2_wins: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedFrame {
    MatchStart(MatchStart),
    Inputs {
        match_id: u32,
        first_tick: u32,
        inputs: Vec<(u16, u16)>,
    },
    Checksum {
        match_id: u32,
        tick: u32,
        checksum: u32,
    },
    MatchEnd {
        match_id: u32,
        final_tick: u32,
        result: MatchResult,
        p1_wins: u8,
        p2_wins: u8,
    },
    FeedReset {
        match_id: u32,
    },
}

impl FeedFrame {
    pub fn match_id(&self) -> u32 {
        match self {
            FeedFrame::MatchStart(m) => m.match_id,
            FeedFrame::Inputs { match_id, .. }
            | FeedFrame::Checksum { match_id, .. }
            | FeedFrame::MatchEnd { match_id, .. }
            | FeedFrame::FeedReset { match_id } => *match_id,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::new();
        match self {
            FeedFrame::MatchStart(m) => {
                out.push(TYPE_MATCH_START);
                le::put_u16(&mut out, m.feed_version);
                le::put_u32(&mut out, m.match_id);
                out.push(m.stage_index);
                out.push(m.p1_fighter_index);
                out.push(m.p2_fighter_index);
                put_str(&mut out, &m.game_version)?;
                put_str(&mut out, &m.stage_id)?;
                put_str(&mut out, &m.p1_fighter_id)?;
                put_str(&mut out, &m.p2_fighter_id)?;
                put_str(&mut out, &m.p1_name)?;
                put_str(&mut out, &m.p2_name)?;
            }
            FeedFrame::Inputs {
                match_id,
                first_tick,
                inputs,
            } => {
                if inputs.is_empty() || inputs.len() > MAX_INPUTS_COUNT as usize {
                    return Err(CodecError::BadValue("inputs count"));
                }
                out.push(TYPE_INPUTS);
                le::put_u32(&mut out, *match_id);
                le::put_u32(&mut out, *first_tick);
                le::put_u16(&mut out, inputs.len() as u16);
                for (p1, p2) in inputs {
                    if !validate_input(*p1) || !validate_input(*p2) {
                        return Err(CodecError::BadValue("input bits"));
                    }
                    le::put_u16(&mut out, *p1);
                    le::put_u16(&mut out, *p2);
                }
            }
            FeedFrame::Checksum {
                match_id,
                tick,
                checksum,
            } => {
                out.push(TYPE_CHECKSUM);
                le::put_u32(&mut out, *match_id);
                le::put_u32(&mut out, *tick);
                le::put_u32(&mut out, *checksum);
            }
            FeedFrame::MatchEnd {
                match_id,
                final_tick,
                result,
                p1_wins,
                p2_wins,
            } => {
                out.push(TYPE_MATCH_END);
                le::put_u32(&mut out, *match_id);
                le::put_u32(&mut out, *final_tick);
                out.push(result.to_byte());
                out.push(*p1_wins);
                out.push(*p2_wins);
            }
            FeedFrame::FeedReset { match_id } => {
                out.push(TYPE_FEED_RESET);
                le::put_u32(&mut out, *match_id);
            }
        }
        Ok(out)
    }

    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let type_byte = *buf.first().ok_or(CodecError::Truncated)?;
        let mut pos = 1usize;
        let frame = match type_byte {
            TYPE_MATCH_START => {
                let feed_version = le::get_u16(buf, &mut pos)?;
                if feed_version != FEED_VERSION {
                    return Err(CodecError::BadValue("feed version"));
                }
                let match_id = le::get_u32(buf, &mut pos)?;
                let stage_index = le::get_u8(buf, &mut pos)?;
                let p1_fighter_index = le::get_u8(buf, &mut pos)?;
                let p2_fighter_index = le::get_u8(buf, &mut pos)?;
                let game_version = get_str(buf, &mut pos)?;
                let stage_id = get_str(buf, &mut pos)?;
                let p1_fighter_id = get_str(buf, &mut pos)?;
                let p2_fighter_id = get_str(buf, &mut pos)?;
                let p1_name = get_str(buf, &mut pos)?;
                let p2_name = get_str(buf, &mut pos)?;
                FeedFrame::MatchStart(MatchStart {
                    feed_version,
                    match_id,
                    stage_index,
                    p1_fighter_index,
                    p2_fighter_index,
                    game_version,
                    stage_id,
                    p1_fighter_id,
                    p2_fighter_id,
                    p1_name,
                    p2_name,
                })
            }
            TYPE_INPUTS => {
                let match_id = le::get_u32(buf, &mut pos)?;
                let first_tick = le::get_u32(buf, &mut pos)?;
                let count = le::get_u16(buf, &mut pos)?;
                if count == 0 || count > MAX_INPUTS_COUNT {
                    return Err(CodecError::BadValue("inputs count"));
                }
                let mut inputs = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let p1 = le::get_u16(buf, &mut pos)?;
                    let p2 = le::get_u16(buf, &mut pos)?;
                    if !validate_input(p1) || !validate_input(p2) {
                        return Err(CodecError::BadValue("input bits"));
                    }
                    inputs.push((p1, p2));
                }
                FeedFrame::Inputs {
                    match_id,
                    first_tick,
                    inputs,
                }
            }
            TYPE_CHECKSUM => {
                let match_id = le::get_u32(buf, &mut pos)?;
                let tick = le::get_u32(buf, &mut pos)?;
                let checksum = le::get_u32(buf, &mut pos)?;
                FeedFrame::Checksum {
                    match_id,
                    tick,
                    checksum,
                }
            }
            TYPE_MATCH_END => {
                let match_id = le::get_u32(buf, &mut pos)?;
                let final_tick = le::get_u32(buf, &mut pos)?;
                let result = MatchResult::from_byte(le::get_u8(buf, &mut pos)?)?;
                let p1_wins = le::get_u8(buf, &mut pos)?;
                let p2_wins = le::get_u8(buf, &mut pos)?;
                FeedFrame::MatchEnd {
                    match_id,
                    final_tick,
                    result,
                    p1_wins,
                    p2_wins,
                }
            }
            TYPE_FEED_RESET => {
                let match_id = le::get_u32(buf, &mut pos)?;
                FeedFrame::FeedReset { match_id }
            }
            other => return Err(CodecError::BadType(other)),
        };
        if pos != buf.len() {
            return Err(CodecError::BadLength);
        }
        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inputs_vector_matches_spec() {
        let frame = FeedFrame::Inputs {
            match_id: 7,
            first_tick: 0,
            inputs: vec![(0x0016, 0x0005), (0x0016, 0x0005)],
        };
        let expected = vec![
            0x02, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x16, 0x00, 0x05,
            0x00, 0x16, 0x00, 0x05, 0x00,
        ];
        assert_eq!(frame.encode().unwrap(), expected);
        assert_eq!(FeedFrame::decode(&expected).unwrap(), frame);
    }

    #[test]
    fn match_start_round_trip() {
        let frame = FeedFrame::MatchStart(MatchStart {
            feed_version: 1,
            match_id: 42,
            stage_index: 4,
            p1_fighter_index: 4,
            p2_fighter_index: 3,
            game_version: "0.4.1".into(),
            stage_id: "beach".into(),
            p1_fighter_id: "jin".into(),
            p2_fighter_id: "valka".into(),
            p1_name: "Lino".into(),
            p2_name: "Lufi".into(),
        });
        let bytes = frame.encode().unwrap();
        assert_eq!(FeedFrame::decode(&bytes).unwrap(), frame);
    }

    #[test]
    fn rejects_bad_input_bits() {
        // Bit 9 set is invalid.
        let bad = FeedFrame::Inputs {
            match_id: 1,
            first_tick: 0,
            inputs: vec![(0x0205, 0x0005)],
        };
        assert!(bad.encode().is_err());
        // Direction 0 is invalid.
        assert!(!validate_input(0x0000));
        // Direction 10 (0xA) is invalid.
        assert!(!validate_input(0x000A));
        // Neutral 5 plus a button is valid.
        assert!(validate_input(0x0015));
        // Sidestep is bit 8.
        assert!(validate_input(0x0105));
    }

    #[test]
    fn checksum_and_end_round_trip() {
        let c = FeedFrame::Checksum {
            match_id: 3,
            tick: 120,
            checksum: 0xDEAD_BEEF,
        };
        assert_eq!(FeedFrame::decode(&c.encode().unwrap()).unwrap(), c);

        let e = FeedFrame::MatchEnd {
            match_id: 3,
            final_tick: 5000,
            result: MatchResult::P1Won,
            p1_wins: 2,
            p2_wins: 1,
        };
        assert_eq!(FeedFrame::decode(&e.encode().unwrap()).unwrap(), e);

        let r = FeedFrame::FeedReset { match_id: 3 };
        assert_eq!(FeedFrame::decode(&r.encode().unwrap()).unwrap(), r);
    }

    #[test]
    fn rejects_trailing_bytes_and_unknown_type() {
        let mut bytes = FeedFrame::FeedReset { match_id: 1 }.encode().unwrap();
        bytes.push(0);
        assert_eq!(FeedFrame::decode(&bytes), Err(CodecError::BadLength));
        assert_eq!(
            FeedFrame::decode(&[0x09, 0, 0, 0, 0]),
            Err(CodecError::BadType(0x09))
        );
    }
}
