use super::{
    command::Command,
    types::{
        ErrorResponse,
        Keyword,
        Request,
        Usage,
    },
};

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
    use crate::protocol::{
        ErrorResponse,
        Request,
        Usage,
        command::Command,
        parse_request,
    };

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
