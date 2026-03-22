use std::io;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{command::Command, database::ExecuteResult};

pub const WELCOME_LINE: &str = "WELCOME AcorusDB";

const PING_KEYWORD: &str = "PING";
const SET_KEYWORD: &str = "SET";
const GET_KEYWORD: &str = "GET";
const EXISTS_KEYWORD: &str = "EXISTS";
const DEL_KEYWORD: &str = "DEL";
const QUIT_KEYWORD: &str = "QUIT";
const EXIT_KEYWORD: &str = "EXIT";

const OK_LINE: &str = "OK";
const PONG_LINE: &str = "PONG";
const NIL_LINE: &str = "(nil)";
const TRUE_LINE: &str = "1";
const FALSE_LINE: &str = "0";
const BYE_LINE: &str = "BYE";
const UNKNOWN_COMMAND_LINE: &str = "ERR unknown command";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Ping,
    Set,
    Get,
    Exists,
    Del,
    Quit,
    Exit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Ping,
    Command(Command),
    Exit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Ok,
    Pong,
    Value(String),
    Nil,
    Boolean(bool),
    Bye,
    Error(ErrorResponse),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorResponse {
    UnknownCommand,
    InvalidArguments(Usage),
}

impl Keyword {
    pub fn parse(input: &str) -> Option<Self> {
        if input.eq_ignore_ascii_case(PING_KEYWORD) {
            Some(Self::Ping)
        } else if input.eq_ignore_ascii_case(SET_KEYWORD) {
            Some(Self::Set)
        } else if input.eq_ignore_ascii_case(GET_KEYWORD) {
            Some(Self::Get)
        } else if input.eq_ignore_ascii_case(EXISTS_KEYWORD) {
            Some(Self::Exists)
        } else if input.eq_ignore_ascii_case(DEL_KEYWORD) {
            Some(Self::Del)
        } else if input.eq_ignore_ascii_case(QUIT_KEYWORD) {
            Some(Self::Quit)
        } else if input.eq_ignore_ascii_case(EXIT_KEYWORD) {
            Some(Self::Exit)
        } else {
            None
        }
    }
}

impl Response {
    pub fn should_close(&self) -> bool {
        matches!(self, Self::Bye)
    }
}

impl From<ExecuteResult> for Response {
    fn from(result: ExecuteResult) -> Self {
        match result {
            ExecuteResult::Set => Self::Ok,
            ExecuteResult::Get(Some(value)) => Self::Value(value),
            ExecuteResult::Get(None) => Self::Nil,
            ExecuteResult::Exists(exists) | ExecuteResult::Delete(exists) => Self::Boolean(exists),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Usage {
    Ping,
    Set,
    Get,
    Exists,
    Del,
    Exit,
}

impl Usage {
    pub const fn error_line(self) -> &'static str {
        match self {
            Self::Ping => "ERR usage: PING",
            Self::Set => "ERR usage: SET key value",
            Self::Get => "ERR usage: GET key",
            Self::Exists => "ERR usage: EXISTS key",
            Self::Del => "ERR usage: DEL key",
            Self::Exit => "ERR usage: EXIT",
        }
    }
}

pub fn parse_request(line: &str) -> std::result::Result<Option<Request>, ErrorResponse> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let (cmd, args) = split_once_whitespace(trimmed).unwrap_or((trimmed, ""));
    match Keyword::parse(cmd) {
        Some(Keyword::Ping) => {
            if args.trim().is_empty() {
                Ok(Some(Request::Ping))
            } else {
                Err(ErrorResponse::InvalidArguments(Usage::Ping))
            }
        }
        Some(Keyword::Set) => {
            if let Some((key, value)) = split_once_whitespace(args) {
                Ok(Some(Request::Command(Command::Set {
                    key: key.into(),
                    value: value.into(),
                })))
            } else {
                Err(ErrorResponse::InvalidArguments(Usage::Set))
            }
        }
        Some(Keyword::Get) => {
            if let Some(key) = single_arg(args) {
                Ok(Some(Request::Command(Command::Get { key: key.into() })))
            } else {
                Err(ErrorResponse::InvalidArguments(Usage::Get))
            }
        }
        Some(Keyword::Exists) => {
            if let Some(key) = single_arg(args) {
                Ok(Some(Request::Command(Command::Exists { key: key.into() })))
            } else {
                Err(ErrorResponse::InvalidArguments(Usage::Exists))
            }
        }
        Some(Keyword::Del) => {
            if let Some(key) = single_arg(args) {
                Ok(Some(Request::Command(Command::Del { key: key.into() })))
            } else {
                Err(ErrorResponse::InvalidArguments(Usage::Del))
            }
        }
        Some(Keyword::Exit | Keyword::Quit) => {
            if args.trim().is_empty() {
                Ok(Some(Request::Exit))
            } else {
                Err(ErrorResponse::InvalidArguments(Usage::Exit))
            }
        }
        None => Err(ErrorResponse::UnknownCommand),
    }
}

pub async fn write_line<W>(writer: &mut W, line: &str) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await
}

pub async fn write_response<W>(writer: &mut W, response: &Response) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_line(writer, response_line(response)).await
}

fn response_line(response: &Response) -> &str {
    match response {
        Response::Ok => OK_LINE,
        Response::Pong => PONG_LINE,
        Response::Value(value) => value.as_str(),
        Response::Nil => NIL_LINE,
        Response::Boolean(true) => TRUE_LINE,
        Response::Boolean(false) => FALSE_LINE,
        Response::Bye => BYE_LINE,
        Response::Error(error) => error_line(*error),
    }
}

fn error_line(error: ErrorResponse) -> &'static str {
    match error {
        ErrorResponse::UnknownCommand => UNKNOWN_COMMAND_LINE,
        ErrorResponse::InvalidArguments(usage) => usage.error_line(),
    }
}

fn split_once_whitespace(input: &str) -> Option<(&str, &str)> {
    let trimmed = input.trim();
    let split_at = trimmed.find(char::is_whitespace)?;
    let (head, tail) = trimmed.split_at(split_at);
    Some((head, tail.trim_start()))
}

fn single_arg(input: &str) -> Option<&str> {
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed.contains(char::is_whitespace) {
        return None;
    }

    Some(trimmed)
}

#[cfg(test)]
mod tests {
    use super::{ErrorResponse, Request, Usage, parse_request};
    use crate::command::Command;

    #[test]
    fn parse_ping_without_args() {
        assert_eq!(parse_request("PING"), Ok(Some(Request::Ping)));
    }

    #[test]
    fn parse_set_keeps_spaces_in_value() {
        assert_eq!(
            parse_request("SET greeting hello world"),
            Ok(Some(Request::Command(Command::Set {
                key: "greeting".into(),
                value: "hello world".into(),
            })))
        );
    }

    #[test]
    fn parse_empty_line_is_ignored() {
        assert_eq!(parse_request("   "), Ok(None));
    }

    #[test]
    fn parse_invalid_get_reports_usage() {
        assert_eq!(
            parse_request("GET one two"),
            Err(ErrorResponse::InvalidArguments(Usage::Get))
        );
    }

    #[test]
    fn parse_exists_requires_single_key() {
        assert_eq!(
            parse_request("EXISTS one two"),
            Err(ErrorResponse::InvalidArguments(Usage::Exists))
        );
    }

    #[test]
    fn parse_unknown_command_reports_error() {
        assert_eq!(parse_request("HELLO"), Err(ErrorResponse::UnknownCommand));
    }
}
