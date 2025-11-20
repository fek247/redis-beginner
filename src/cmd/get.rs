pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string()
        }
    }
}