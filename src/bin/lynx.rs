use std::{path::PathBuf, sync::Arc};

use clap::{Parser, Subcommand};
use lynx::{
    query::QueryFormat,
    server::{
        self, Persistence, ServerRunConfig, LYNX_FORMAT_HEADER, V1_INGEST_PATH, V1_QUERY_PATH,
    },
};
use object_store::ObjectStore;
use reqwest::{header::CONTENT_TYPE, StatusCode};

#[derive(Debug, Clone, Parser)]
struct Cli {
    #[clap(subcommand)]
    commands: Commands,
}

#[derive(Debug, Clone, Parser)]
struct AwsOptions {
    /// Region that the bucket resides in.
    #[arg(
        long = "aws-region",
        required_if_eq("persist_mode", "s3"),
        env = "AWS_REGION"
    )]
    region: Option<String>,

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

    #[arg(
        long = "aws-access-key-id",
        required_if_eq("persist_mode", "s3"),
        env = "AWS_ACCESS_KEY_ID"
    )]
    access_key_id: Option<String>,

    #[arg(
        long = "aws-secret-access-key",
        required_if_eq("persist_mode", "s3"),
        env = "AWS_SECRET_ACCESS_KEY"
    )]
    secret_access_key: Option<String>,

    /// Allow insecure (non-HTTPS) connections to the bucket.
    #[arg(long = "aws-allow-http", env = "AWS_ALLOW_HTTP")]
    allow_http: Option<bool>,
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

        #[clap(flatten)]
        aws: AwsOptions,
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
            eprintln!("Persist mode: {}", persist_mode.as_str());
            eprintln!("Persist path: {}", persist_path.display());

            let config = ServerRunConfig::new(
                &host,
                port,
                events_before_persist,
                persist_path,
                Arc::new(object_store),
                persist_mode,
            );
            server::run(config).await?;
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
