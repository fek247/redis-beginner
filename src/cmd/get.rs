use bytes::Bytes;
use crate::{Frame, parse::Parse};

pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string()
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn parse_frame(mut parse: Parse) -> Get {
        let key = parse.next_string().unwrap();
        Get { key }
    }
}