mod parse;
use parse::{Parse, ParseError};

mod cmd;
pub use cmd::NewCommand;

mod frame;
pub use frame::Frame;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;