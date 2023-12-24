use std::ops::Range;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    pub fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    pub fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

impl From<Range<u64>> for CommandPos {
    fn from(range: Range<u64>) -> Self {
        CommandPos {
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

#[derive(Debug)]
pub struct CommandPos {
    pub pos: u64,
    pub len: u64,
}
