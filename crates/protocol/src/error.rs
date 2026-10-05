use std::fmt;

/// A malformed binary frame or datagram. Every codec returns this instead of
/// panicking on untrusted input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// The buffer ended before all declared fields were read.
    Truncated,
    /// The leading type byte is not one this codec understands.
    BadType(u8),
    /// The buffer is longer or shorter than the layout allows.
    BadLength,
    /// A field is out of its permitted range.
    BadValue(&'static str),
    /// A string field was not valid UTF-8.
    Utf8,
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::Truncated => write!(f, "buffer truncated"),
            CodecError::BadType(t) => write!(f, "unknown type byte 0x{t:02x}"),
            CodecError::BadLength => write!(f, "wrong buffer length"),
            CodecError::BadValue(what) => write!(f, "invalid value: {what}"),
            CodecError::Utf8 => write!(f, "invalid utf-8"),
        }
    }
}

impl std::error::Error for CodecError {}

/// Little-endian readers/writers shared by the binary codecs.
pub(crate) mod le {
    use super::CodecError;

    pub fn put_u16(out: &mut Vec<u8>, v: u16) {
        out.extend_from_slice(&v.to_le_bytes());
    }

    pub fn put_u32(out: &mut Vec<u8>, v: u32) {
        out.extend_from_slice(&v.to_le_bytes());
    }

    pub fn get_u8(buf: &[u8], pos: &mut usize) -> Result<u8, CodecError> {
        let v = *buf.get(*pos).ok_or(CodecError::Truncated)?;
        *pos += 1;
        Ok(v)
    }

    pub fn get_u16(buf: &[u8], pos: &mut usize) -> Result<u16, CodecError> {
        let end = pos.checked_add(2).ok_or(CodecError::Truncated)?;
        let slice = buf.get(*pos..end).ok_or(CodecError::Truncated)?;
        *pos = end;
        Ok(u16::from_le_bytes([slice[0], slice[1]]))
    }

    pub fn get_u32(buf: &[u8], pos: &mut usize) -> Result<u32, CodecError> {
        let end = pos.checked_add(4).ok_or(CodecError::Truncated)?;
        let slice = buf.get(*pos..end).ok_or(CodecError::Truncated)?;
        *pos = end;
        Ok(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
    }

    pub fn get_bytes<const N: usize>(buf: &[u8], pos: &mut usize) -> Result<[u8; N], CodecError> {
        let end = pos.checked_add(N).ok_or(CodecError::Truncated)?;
        let slice = buf.get(*pos..end).ok_or(CodecError::Truncated)?;
        let mut out = [0u8; N];
        out.copy_from_slice(slice);
        *pos = end;
        Ok(out)
    }
}
