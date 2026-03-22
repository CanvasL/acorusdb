use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "acorusdb")]
#[command(version)]
#[command(about = "A lightweight TCP key-value database")]
pub struct Cli {
    #[arg(short, long, default_value = "acorusdb.toml")]
    pub config: PathBuf,
}

impl Cli {
    pub fn parse_args() -> Self {
        <Self as Parser>::parse()
    }
}
