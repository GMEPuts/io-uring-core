//! WebSocket (RFC 6455) client framing and the HTTP/1.1 upgrade. No I/O.

use std::io::Write;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpCode {
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
}

impl OpCode {
    #[inline]
    fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0x0 => Self::Continuation,
            0x1 => Self::Text,
            0x2 => Self::Binary,
            0x8 => Self::Close,
            0x9 => Self::Ping,
            0xA => Self::Pong,
            _ => return None,
        })
    }

    #[inline]
    fn is_control(self) -> bool {
        matches!(self, Self::Close | Self::Ping | Self::Pong)
    }
}

/// A protocol violation by the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsError {
    /// Unknown opcode.
    BadOpcode(u8),
    /// RSV bits set: the server is compressing (permessage-deflate is never offered).
    ReservedBits,
    /// Servers must not mask frames.
    MaskedFromServer,
    /// Control frame fragmented or longer than 125 bytes.
    BadControlFrame,
    /// Continuation without a started message, or a new message inside a fragmented one.
    BadFragment,
    /// Length doesn't fit in memory.
    TooLarge,
}

/// A frame borrowed from the receive buffer; `len` is header + payload, i.e. what to consume.
pub(crate) struct Frame<'a> {
    pub fin: bool,
    pub opcode: OpCode,
    pub payload: &'a [u8],
    pub len: usize,
}

/// `Ok(None)` means the frame is not complete yet.
#[inline]
pub(crate) fn parse_frame(buf: &[u8]) -> Result<Option<Frame<'_>>, WsError> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let (b0, b1) = (buf[0], buf[1]);
    if b0 & 0x70 != 0 {
        return Err(WsError::ReservedBits);
    }
    if b1 & 0x80 != 0 {
        return Err(WsError::MaskedFromServer);
    }
    let fin = b0 & 0x80 != 0;
    let opcode = OpCode::from_u8(b0 & 0x0F).ok_or(WsError::BadOpcode(b0 & 0x0F))?;

    let (payload_len, header_len): (usize, usize) = match b1 & 0x7F {
        126 => {
            if buf.len() < 4 {
                return Ok(None);
            }
            (u16::from_be_bytes([buf[2], buf[3]]) as usize, 4)
        }
        127 => {
            if buf.len() < 10 {
                return Ok(None);
            }
            let n = u64::from_be_bytes(buf[2..10].try_into().unwrap());
            (usize::try_from(n).map_err(|_| WsError::TooLarge)?, 10)
        }
        n => (n as usize, 2),
    };

    if opcode.is_control() && (!fin || payload_len > 125) {
        return Err(WsError::BadControlFrame);
    }
    let len = header_len.checked_add(payload_len).ok_or(WsError::TooLarge)?;
    if buf.len() < len {
        return Ok(None);
    }
    Ok(Some(Frame { fin, opcode, payload: &buf[header_len..len], len }))
}

/// Appends a masked client frame to `out`.
pub(crate) fn encode_frame(opcode: OpCode, payload: &[u8], mask: [u8; 4], out: &mut Vec<u8>) {
    out.push(0x80 | opcode as u8);
    let len = payload.len();
    if len <= 125 {
        out.push(0x80 | len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0x80 | 126);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0x80 | 127);
        out.extend_from_slice(&(len as u64).to_be_bytes());
    }
    out.extend_from_slice(&mask);
    let start = out.len();
    out.extend_from_slice(payload);
    apply_mask(&mut out[start..], mask);
}

#[inline]
fn apply_mask(payload: &mut [u8], mask: [u8; 4]) {
    let m = u64::from_ne_bytes([mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3]]);
    let mut chunks = payload.chunks_exact_mut(8);
    for chunk in &mut chunks {
        let v = u64::from_ne_bytes((&*chunk).try_into().unwrap()) ^ m;
        chunk.copy_from_slice(&v.to_ne_bytes());
    }
    for (i, b) in chunks.into_remainder().iter_mut().enumerate() {
        *b ^= mask[i & 3];
    }
}

pub(crate) fn new_key() -> String {
    let mut key = [0u8; 16];
    fastrand::fill(&mut key);
    STANDARD.encode(key)
}

/// Cloudflare-fronted endpoints have silently dropped headerless upgrades, so a browser-like
/// User-Agent is sent unless the caller sets one.
const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36";

/// Writes the upgrade GET. `headers` are the caller's extras (they override the default
/// User-Agent). permessage-deflate is never offered: compressed frames are rejected.
pub(crate) fn write_upgrade_request(out: &mut Vec<u8>, host: &str, path: &str, key: &str, headers: &[(String, String)]) {
    let _ = write!(
        out,
        "GET {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Key: {key}\r\n\
         Pragma: no-cache\r\n\
         Cache-Control: no-cache\r\n"
    );
    if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("user-agent")) {
        let _ = write!(out, "User-Agent: {DEFAULT_USER_AGENT}\r\n");
    }
    for (k, v) in headers {
        let _ = write!(out, "{k}: {v}\r\n");
    }
    out.extend_from_slice(b"\r\n");
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Upgrade {
    Incomplete,
    Accepted { header_len: usize },
    Rejected { status: u16 },
}

pub(crate) fn parse_upgrade_response(buf: &[u8]) -> Upgrade {
    let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
        return Upgrade::Incomplete;
    };
    // "HTTP/1.1 101 Switching Protocols"
    let status = buf
        .get(9..12)
        .and_then(|s| std::str::from_utf8(s).ok())
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    if buf.starts_with(b"HTTP/1.1 ") && status == 101 {
        Upgrade::Accepted { header_len: end + 4 }
    } else {
        Upgrade::Rejected { status }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server_frame(fin: bool, opcode: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![(if fin { 0x80 } else { 0 }) | opcode];
        let n = payload.len();
        if n <= 125 {
            v.push(n as u8);
        } else if n <= 65535 {
            v.push(126);
            v.extend_from_slice(&(n as u16).to_be_bytes());
        } else {
            v.push(127);
            v.extend_from_slice(&(n as u64).to_be_bytes());
        }
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn parses_frames_of_each_length_class() {
        for n in [0usize, 5, 125, 126, 1000, 65535, 70_000] {
            let payload = vec![b'x'; n];
            let buf = server_frame(true, 0x1, &payload);
            let f = parse_frame(&buf).unwrap().unwrap();
            assert!(f.fin);
            assert_eq!(f.opcode, OpCode::Text);
            assert_eq!(f.payload.len(), n);
            assert_eq!(f.len, buf.len());
        }
    }

    #[test]
    fn partial_frames_are_incomplete_at_every_split() {
        let buf = server_frame(true, 0x2, &vec![7u8; 300]);
        for cut in 0..buf.len() {
            assert!(parse_frame(&buf[..cut]).unwrap().is_none(), "cut at {cut}");
        }
        assert!(parse_frame(&buf).unwrap().is_some());
    }

    #[test]
    fn rejects_protocol_violations() {
        assert_eq!(parse_frame(&[0x81 | 0x40, 0]).err(), Some(WsError::ReservedBits));
        assert_eq!(parse_frame(&[0x81, 0x80]).err(), Some(WsError::MaskedFromServer));
        assert_eq!(parse_frame(&[0x83, 0]).err(), Some(WsError::BadOpcode(3)));
        assert_eq!(parse_frame(&[0x09, 0]).err(), Some(WsError::BadControlFrame)); // fragmented ping
        assert_eq!(parse_frame(&server_frame(true, 0x9, &[0; 126])).err(), Some(WsError::BadControlFrame));
    }

    #[test]
    fn encodes_masked_frames() {
        for n in [0usize, 3, 125, 126, 65535, 65536] {
            let payload: Vec<u8> = (0..n).map(|i| i as u8).collect();
            let mask = [1, 2, 3, 4];
            let mut out = Vec::new();
            encode_frame(OpCode::Text, &payload, mask, &mut out);
            assert_eq!(out[0], 0x81);
            assert!(out[1] & 0x80 != 0);
            let (len, hdr) = match out[1] & 0x7F {
                126 => (u16::from_be_bytes([out[2], out[3]]) as usize, 4),
                127 => (u64::from_be_bytes(out[2..10].try_into().unwrap()) as usize, 10),
                n => (n as usize, 2),
            };
            assert_eq!(len, n);
            assert_eq!(&out[hdr..hdr + 4], &mask);
            let unmasked: Vec<u8> = out[hdr + 4..].iter().enumerate().map(|(i, b)| b ^ mask[i & 3]).collect();
            assert_eq!(unmasked, payload);
        }
    }

    #[test]
    fn upgrade_request_headers() {
        let mut out = Vec::new();
        write_upgrade_request(&mut out, "h:9443", "/ws?x=1", "KEY", &[]);
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("GET /ws?x=1 HTTP/1.1\r\nHost: h:9443\r\n"));
        assert!(s.contains("Sec-WebSocket-Key: KEY\r\n"));
        assert!(s.contains("User-Agent: Mozilla"));
        assert!(s.ends_with("\r\n\r\n"));

        let mut out = Vec::new();
        write_upgrade_request(&mut out, "h", "/", "K", &[("user-agent".into(), "me".into()), ("X-A".into(), "1".into())]);
        let s = String::from_utf8(out).unwrap();
        assert!(!s.contains("Mozilla"));
        assert!(s.contains("user-agent: me\r\nX-A: 1\r\n\r\n"));
    }

    #[test]
    fn upgrade_response() {
        assert_eq!(parse_upgrade_response(b"HTTP/1.1 101 Switching"), Upgrade::Incomplete);
        let ok = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n\x81\x00";
        assert_eq!(parse_upgrade_response(ok), Upgrade::Accepted { header_len: ok.len() - 2 });
        assert_eq!(parse_upgrade_response(b"HTTP/1.1 403 Forbidden\r\n\r\n"), Upgrade::Rejected { status: 403 });
    }
}
