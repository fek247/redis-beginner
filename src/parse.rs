use std::{fmt, vec};
use bytes::Bytes;

use crate::Frame;

#[derive(Debug)]
pub(crate) struct Parse {
    parts: vec::IntoIter<Frame>,
}

#[derive(Debug)]
pub(crate) enum ParseError {
    EndOfStream,
    Other(crate::Error),
}

impl Parse {
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("protocol error; expected array, got {:?}", frame).into()),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    pub fn next(&mut self) -> Result<Frame, ParseError> {
        match self.parts.next() {
            Some(frame) => Ok(frame),
            None => Err(ParseError::EndOfStream),
        }
    }

    pub fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            frame => Err(ParseError::Other(format!(
                "Expected simple string frame, got {:?}",
                frame
            )
            .into())),
        }
    }

    pub fn next_int(&mut self) -> Result<i64, ParseError> {
        match self.next()? {
            Frame::Integer(i) => Ok(i),
            frame => Err(ParseError::Other(format!(
                "Expected integer frame, got {:?}",
                frame
            )
            .into())),
        }
    }

    pub fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(Bytes::from(s)),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!("Expected simple string or bulk string frame, got {:?}", frame).into()),
        }
    }
}

impl From<String> for ParseError {
    fn from(src: String) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        src.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error; unexpected end of stream".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}