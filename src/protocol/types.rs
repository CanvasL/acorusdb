use crate::{
    database::ExecuteResult,
    protocol::command::Command,
};

pub const WELCOME_LINE: &str = "WELCOME AcorusDB";

const PING_KEYWORD: &str = "PING";
const SET_KEYWORD: &str = "SET";
const GET_KEYWORD: &str = "GET";
const EXISTS_KEYWORD: &str = "EXISTS";
const DEL_KEYWORD: &str = "DEL";
const QUIT_KEYWORD: &str = "QUIT";
const EXIT_KEYWORD: &str = "EXIT";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Usage {
    Ping,
    Set,
    Get,
    Exists,
    Del,
    Exit,
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
