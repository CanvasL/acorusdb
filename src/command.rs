use crate::storage_engine::StorageEngine;

use std::io::Result;

mod command_prefix {
    pub const SET: &str = "SET";
    pub const GET: &str = "GET";
    pub const DEL: &str = "DEL";
    pub const QUIT: &str = "QUIT";
    pub const EXIT: &str = "EXIT";
}

pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Del { key: String },
    Exit,
    Unknown,
}

impl Command {
    pub fn parse(line: &str) -> Self {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Command::Unknown;
        }

        let (cmd, args) = split_once_whitespace(trimmed).unwrap_or((trimmed, ""));
        match cmd.to_ascii_uppercase().as_str() {
            command_prefix::SET => {
                if let Some((key, value)) = split_once_whitespace(args) {
                    Command::Set {
                        key: key.into(),
                        value: value.into(),
                    }
                } else {
                    Command::Unknown
                }
            }
            command_prefix::GET => {
                if let Some(key) = single_arg(args) {
                    Command::Get { key: key.into() }
                } else {
                    Command::Unknown
                }
            }
            command_prefix::DEL => {
                if let Some(key) = single_arg(args) {
                    Command::Del { key: key.into() }
                } else {
                    Command::Unknown
                }
            }
            command_prefix::EXIT | command_prefix::QUIT => Command::Exit,
            _ => Command::Unknown,
        }
    }

    pub fn execute(&self, storage_engine: &mut StorageEngine) -> Result<String> {
        match self {
            Command::Set { key, value } => {
                storage_engine.set(key, value)?;
                Ok("OK".into())
            }
            Command::Get { key } => {
                let value = storage_engine.get(key).cloned();
                match value {
                    Some(value) => Ok(value),
                    None => Ok("(nil)".into()),
                }
            }
            Command::Del { key } => {
                let deleted = storage_engine.delete(key)?;
                Ok(if deleted { "1" } else { "0" }.into())
            }
            Command::Exit => Ok("BYE".into()),
            Command::Unknown => Ok("ERR unknown command".into()),
        }
    }
}

/// Splits the input string into two parts at the first occurrence of whitespace.
/// Returns None if there is no whitespace in the input.
fn split_once_whitespace(input: &str) -> Option<(&str, &str)> {
    let trimmed = input.trim();
    let split_at = trimmed.find(char::is_whitespace)?;
    let (head, tail) = trimmed.split_at(split_at);
    Some((head, tail.trim_start()))
}

/// Validates that the input is a single argument (no whitespace) and returns it.
fn single_arg(input: &str) -> Option<&str> {
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed.contains(char::is_whitespace) {
        return None;
    }

    Some(trimmed)
}
