use std::{path::PathBuf, sync::Arc};

use clap::{Parser, Subcommand};
use lynx::{
    query::QueryFormat,
    server::{self, Persistence, LYNX_FORMAT_HEADER, V1_INGEST_PATH, V1_QUERY_PATH},
};
use reqwest::{header::CONTENT_TYPE, StatusCode};

#[derive(Debug, Clone, Parser)]
struct Cli {
    #[clap(subcommand)]
    commands: Commands,
}

#[derive(Debug, Clone, Subcommand)]
enum Commands {
    /// Run the lynx server
    Server {
        #[arg(long, env = "LYNX_HOST", default_value = "127.0.0.1")]
        host: String,

        #[arg(long, env = "LYNX_PORT", default_value = "3000")]
        port: u16,

        /// Number of events before a persist event occurs per namespace.
        #[arg(long, env = "LYNX_PERSIST_EVENTS", default_value = "2")]
        events_before_persist: i64,

        /// Path where lynx will persist parquet files.
        #[arg(long, env = "LYNX_PERSIST_PATH", default_value = "/tmp")]
        persist_path: PathBuf,

        /// Dictate where parquet files will be persisted.
        ///
        /// All modes except 'local' require extra configuration.
        #[arg(long, env = "LYNX_PERSIST_MODE", default_value = "local")]
        persist_mode: Persistence,
    },
    /// Write data to lynx
    Write {
        #[arg(long, env = "LYNX_HOST", default_value = "127.0.0.1")]
        host: String,

        #[arg(long, env = "LYNX_PORT", default_value = "3000")]
        port: u16,

        /// Path to a JSON file containing event(s) to ingest.
        #[arg(long)]
        file: PathBuf,
    },
    /// Query data from lynx
    Query {
        #[arg(long, env = "LYNX_HOST", default_value = "127.0.0.1")]
        host: String,

        #[arg(long, env = "LYNX_PORT", default_value = "3000")]
        port: u16,

        /// Path to a JSON file containing query information.
        #[arg(long)]
        file: PathBuf,

        #[arg(long, default_value = "json")]
        format: QueryFormat,
    },
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
            persist_mode,
        } => {
            let object_store = match persist_mode {
                Persistence::Local => {
                    object_store::local::LocalFileSystem::new_with_prefix(&persist_path)?
                }
                Persistence::S3 => todo!(),
            };
            server::run(
                &host,
                port,
                events_before_persist,
                persist_path,
                Arc::new(object_store),
            )
            .await?;
        }
        Commands::Write { host, port, file } => {
            let json = std::fs::read(&file).unwrap();
            let client = reqwest::Client::new();

            let resp = client
                .post(format!("http://{host}:{port}/{V1_INGEST_PATH}"))
                .header(CONTENT_TYPE, "application/json")
                .body(json)
                .send()
                .await?;

            match resp.status() {
                StatusCode::CREATED => {}
                code => {
                    return Err(format!(
                        "received unexpected response {code}, body: {}",
                        resp.text().await.unwrap()
                    )
                    .into())
                }
            }
        }
        Commands::Query {
            host,
            port,
            file,
            format,
        } => {
            let json = std::fs::read(&file)?;
            let client = reqwest::Client::new();

            let resp = client
                .post(format!("http://{host}:{port}/{V1_QUERY_PATH}"))
                .header(CONTENT_TYPE, "application/json")
                .header(LYNX_FORMAT_HEADER, format.as_str())
                .body(json)
                .send()
                .await?;

            println!("{}", resp.text().await?);
        }
    };

    Ok(())
}
