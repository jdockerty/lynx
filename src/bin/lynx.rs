use std::{path::PathBuf, sync::Arc};

use clap::{Parser, Subcommand};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use lynx::{
    query::QueryFormat,
    server::{
        self, Persistence, ServerRunConfig, LYNX_FORMAT_HEADER, V1_INGEST_PATH, V1_QUERY_PATH,
    },
};
use object_store::ObjectStore;
use reqwest::{header::CONTENT_TYPE, StatusCode};
use tracing::{info, warn};

#[derive(Debug, Clone, Parser)]
struct Cli {
    #[clap(subcommand)]
    commands: Commands,

    #[command(flatten)]
    verbose: Verbosity<InfoLevel>,
}

#[derive(Debug, Clone, Parser)]
struct AwsOptions {
    #[arg(
        long = "aws-access-key-id",
        required_if_eq("persist_mode", "s3"),
        env = "AWS_ACCESS_KEY_ID"
    )]
    access_key_id: Option<String>,

    /// Allow insecure (non-HTTPS) connections to the bucket.
    #[arg(long = "aws-allow-http", env = "AWS_ALLOW_HTTP")]
    allow_http: Option<bool>,

    /// Name of the bucket.
    #[arg(
        long = "aws-bucket",
        required_if_eq("persist_mode", "s3"),
        env = "LYNX_BUCKET"
    )]
    bucket: Option<String>,

    /// Endpoint to use for the bucket connection.
    #[arg(long = "aws-endpoint", env = "AWS_ENDPOINT")]
    endpoint: Option<String>,

    /// Region that the bucket resides in.
    #[arg(
        long = "aws-region",
        required_if_eq("persist_mode", "s3"),
        env = "AWS_REGION"
    )]
    region: Option<String>,

    #[arg(
        long = "aws-secret-access-key",
        required_if_eq("persist_mode", "s3"),
        env = "AWS_SECRET_ACCESS_KEY"
    )]
    secret_access_key: Option<String>,
}

#[derive(Debug, Clone, Subcommand)]
enum Commands {
    /// Run the lynx server
    Server {
        #[clap(flatten)]
        aws: AwsOptions,

        /// Number of events before a persist event occurs per namespace.
        #[arg(long, env = "LYNX_PERSIST_EVENTS", default_value = "2")]
        events_before_persist: i64,

        #[arg(long, env = "LYNX_HOST", default_value = "127.0.0.1")]
        host: String,

        /// Dictate where parquet files will be persisted.
        ///
        /// All modes except 'local' require extra configuration.
        #[arg(long, env = "LYNX_PERSIST_MODE", default_value = "local")]
        persist_mode: Persistence,

        /// Path where lynx will persist parquet files.
        #[arg(long, env = "LYNX_PERSIST_PATH", default_value = "/tmp")]
        persist_path: PathBuf,

        /// Path where lynx will persist parquet files.
        #[arg(long, env = "LYNX_WAL_DIR", default_value = "./")]
        wal_dir: PathBuf,

        #[arg(long, env = "LYNX_WAL_BUFFER_SIZE", default_value = "8096")]
        wal_buffer_size: usize,

        #[arg(long, env = "LYNX_PORT", default_value = "3000")]
        port: u16,
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

    tracing_subscriber::fmt::fmt()
        .with_max_level(cli.verbose)
        .init();

    match cli.commands {
        Commands::Server {
            host,
            port,
            events_before_persist,
            persist_path,
            wal_dir,
            wal_buffer_size,
            persist_mode,
            aws,
        } => {
            let object_store: Arc<dyn ObjectStore> = match persist_mode {
                Persistence::Local => Arc::new(
                    object_store::local::LocalFileSystem::new_with_prefix(&persist_path)?,
                ),
                Persistence::S3 => Arc::new(
                    object_store::aws::AmazonS3Builder::new()
                        .with_bucket_name(aws.bucket.as_ref().unwrap())
                        .with_region(aws.region.as_ref().unwrap())
                        .with_endpoint(aws.endpoint.clone().unwrap_or_default())
                        .with_access_key_id(aws.access_key_id.as_ref().unwrap())
                        .with_secret_access_key(aws.secret_access_key.as_ref().unwrap())
                        .with_allow_http(aws.allow_http.unwrap_or_default())
                        .build()?,
                ),
            };

            let persist_path = match persist_mode {
                Persistence::Local => persist_path.join("lynx"),
                Persistence::S3 => format!("{}/lynx", aws.bucket.unwrap()).into(),
            };

            info!(persist_path = %persist_path.display());
            info!(persist_mode = persist_mode.as_str());
            info!(wal_path = %wal_dir.display());
            info!(events_before_persist);

            let config = ServerRunConfig::new(
                &host,
                port,
                events_before_persist,
                persist_path,
                wal_dir,
                wal_buffer_size,
                Arc::new(object_store),
                persist_mode,
            );

            tokio::select! {
                _ = server::run(config) => {
                    warn!("Server process exited");
                },
                _ = tokio::signal::ctrl_c() => {
                    warn!("Shutting down server");
                }
            };
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
