use bytes::Bytes;

#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error),
}

impl Frame {
    pub fn parse(data: &[u8]) -> Option<Frame> {
        match data[0] {
            b'+' => {
                let line = String::from_utf8_lossy(&data[1..data.len() - 2]);
                Some(Frame::Simple(line.to_string()))
            }
            b'-' => {
                let line = String::from_utf8_lossy(&data[1..data.len() - 2]);
                Some(Frame::Error(line.to_string()))
            }
            _ => None,
        }
    }
}
