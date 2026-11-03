#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    Connect = 1,
    Recv = 2,
    Send = 3,
    Wake = 4,
}

/// SQE user_data, packed as [op:8][unused:8][slot:16][gen:32]. `gen` is the connection's socket
/// generation at submit time; it is bumped on every close, so completions from an earlier socket
/// in the same slot are recognised and only release what they hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Token {
    pub op: Op,
    pub slot: u16,
    pub gen: u32,
}

impl Token {
    #[inline]
    pub(crate) const fn new(op: Op, slot: u16, gen: u32) -> Self {
        Self { op, slot, gen }
    }

    #[inline]
    pub(crate) const fn encode(self) -> u64 {
        (self.op as u64) << 56 | (self.slot as u64) << 32 | self.gen as u64
    }

    #[inline]
    pub(crate) fn decode(v: u64) -> Option<Self> {
        let op = match (v >> 56) as u8 {
            1 => Op::Connect,
            2 => Op::Recv,
            3 => Op::Send,
            4 => Op::Wake,
            _ => return None,
        };
        Some(Self { op, slot: (v >> 32) as u16, gen: v as u32 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        for t in [Token::new(Op::Recv, 7, u32::MAX), Token::new(Op::Wake, 0, 0), Token::new(Op::Send, u16::MAX, 12345)] {
            assert_eq!(Token::decode(t.encode()), Some(t));
        }
        assert_eq!(Token::decode(0), None);
    }
}
