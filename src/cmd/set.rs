use std::time::Duration;

use bytes::Bytes;

use crate::parse::Parse;

pub struct Set {
    key: String,
    value: Bytes,
    expire: Option<Duration>,
}

impl Set {
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire
        }
    }

    pub fn parse_frame(mut parse: Parse) -> crate::Result<Set> {
        let key = parse.next_string().unwrap();
        
        let value = parse.next_bytes().unwrap();

        let mut expire = None;

        match parse.next_string() {
            Ok(s) if s.to_lowercase() == "px" => {
                let secs = parse.next_int().unwrap();
                expire = Some(Duration::from_millis(secs as u64));
            }
            Ok(s) if s.to_lowercase() == "ex" => {
                let secs = parse.next_int().unwrap();
                expire = Some(Duration::from_secs(secs as u64));
            }
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            Err(crate::parse::ParseError::EndOfStream) => {}
            Err(err) => return Err(err).unwrap(),
        }

        Ok(Set::new(key, value, expire))
    }
}