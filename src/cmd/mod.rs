mod get;
pub use get::Get;

mod set;
pub use set::Set;

use crate::{Frame, Parse};

pub enum NewCommand {
    Get(Get),
    Set(Set),
}

impl NewCommand {
    pub fn from_frame(frame: Frame) -> crate::Result<NewCommand> {
        let mut parse = Parse::new(frame)?;

        let command_name = parse.next_string()?.to_lowercase();

        let command = match command_name.as_str() {
            "get" => NewCommand::Get(Get::parse_frame(parse)),
            "set" => NewCommand::Set(Set::parse_frame(parse)?),
            _ => return Err(format!("Unknown command: {}", command_name).into()),
        };

        Ok(command)
    }
}