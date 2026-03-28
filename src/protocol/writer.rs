use std::io;

use tokio::io::{
    AsyncWrite,
    AsyncWriteExt,
};

use super::types::{
    ErrorResponse,
    Response,
};

const OK_LINE: &str = "OK";
const PONG_LINE: &str = "PONG";
const NIL_LINE: &str = "(nil)";
const TRUE_LINE: &str = "1";
const FALSE_LINE: &str = "0";
const BYE_LINE: &str = "BYE";
const UNKNOWN_COMMAND_LINE: &str = "ERR unknown command";

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
