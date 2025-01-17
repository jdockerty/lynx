use std::path::PathBuf;

use clap::{Parser, Subcommand};
use lynx::server;

#[derive(Debug, Clone, Parser)]
struct Cli {
    #[clap(subcommand)]
    commands: Commands,
}

#[derive(Debug, Clone, Subcommand)]
enum Commands {
    Server {
        #[arg(long, env = "LYNX_HOST", default_value = "127.0.0.1")]
        host: String,

        #[arg(long, env = "LYNX_PORT", default_value = "3000")]
        port: u16,

        #[arg(long, env = "LYNX_PERSIST_EVENTS", default_value = "2")]
        events_before_persist: i64,

        #[arg(long, env = "LYNX_PERSIST_PATH", default_value = "./")]
        persist_path: PathBuf,
    },
    Write,
    Query,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.commands {
        Commands::Server {
            host,
            port,
            events_before_persist,
            persist_path,
        } => {
            server::run(&host, port, events_before_persist, persist_path.into()).await?;
        }
        Commands::Write => {}
        Commands::Query => {}
    }

    Ok(())
}
