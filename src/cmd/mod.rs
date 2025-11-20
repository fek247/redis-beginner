mod get;
pub use get::Get;

mod set;
pub use set::Set;

pub enum NewCommand {
    Get(Get),
    Set(Set),
}