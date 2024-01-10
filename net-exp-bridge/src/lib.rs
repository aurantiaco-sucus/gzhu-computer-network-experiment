use std::fmt::{Display, Formatter};
use serde::{Serialize, Deserialize};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Address {
    pub data: [u8; 4]
}

impl Display for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let a1 = self.data[0];
        let a2 = self.data[1];
        let a3 = self.data[2];
        let a4 = self.data[3];
        write!(f, "{a1:02x}:{a2:02x}:{a3:02x}:{a4:02x}")
    }
}

impl TryFrom<&str> for Address {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 11 {
            return Err(());
        }
        let mut data = [0; 4];
        data[0] = u8::from_str_radix(&value[0..2], 16).map_err(|_| ())?;
        data[1] = u8::from_str_radix(&value[3..5], 16).map_err(|_| ())?;
        data[2] = u8::from_str_radix(&value[6..8], 16).map_err(|_| ())?;
        data[3] = u8::from_str_radix(&value[9..11], 16).map_err(|_| ())?;
        Ok(Address { data })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Segment {
    pub data: [u8; 2]
}

impl Display for Segment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let a1 = self.data[0];
        let a2 = self.data[1];
        write!(f, "{a1:02x}:{a2:02x}")
    }
}

impl TryFrom<&str> for Segment {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 5 {
            return Err(());
        }
        let mut data = [0; 2];
        data[0] = u8::from_str_radix(&value[0..2], 16).map_err(|_| ())?;
        data[1] = u8::from_str_radix(&value[3..5], 16).map_err(|_| ())?;
        Ok(Segment { data })
    }
}

pub type FrameData = [u8; 4];

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Frame {
    pub src: Address,
    pub src_seg: Segment,
    pub dst: Address,
    pub data: FrameData
}

impl Display for Frame {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {} {:02x?}", self.src, self.src_seg, self.dst, self.data)
    }
}

impl TryFrom<&str> for Frame {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut seg = value.trim().split(' ');
        let src = if let Some(val) = seg.next() { val } else { return Err(()) };
        let src_seg = if let Some(val) = seg.next() { val } else { return Err(()) };
        let dst = if let Some(val) = seg.next() { val } else { return Err(()) };
        let data_s = if let Some(val) = seg.next() { val } else { return Err(()) };
        let mut data = FrameData::default();
        for i in 0..16 {
            data[i] = u8::from_str_radix(&data_s[i * 2..i * 2 + 2], 16).map_err(|_| ())?;
        }
        Ok(Frame {
            src: src.try_into()?,
            src_seg: src_seg.try_into()?,
            dst: dst.try_into()?,
            data
        })
    }
}