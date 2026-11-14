//! `synapta-node` — runs one Synapta node, or generates its key pair.
//!
//! ```text
//! synapta-node run --config <node.toml>
//! synapta-node keygen --out <keys/node-x.key>     # also writes <keys/node-x.pub>
//! ```
//!
//! While running, the node writes into its data dir: `node.log` (JSON lines), `api-token`
//! (local API bearer token, new on every start) and `runtime.json` (bound addresses, pid).

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Mutex;

use synapta_core::NodeConfig;
use synapta_gateway::client::{RuntimeInfo, RUNTIME_FILE, TOKEN_FILE};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

const USAGE: &str = "usage:\n  synapta-node run --config <node.toml>\n  synapta-node keygen --out <path/to/node.key>";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["run", "--config", path] => run(Path::new(path)),
        ["keygen", "--out", path] => keygen(Path::new(path)),
        _ => Err(USAGE.to_string()),
    };
    match result {
        Ok(code) => code,
        Err(message) => {
            eprintln!("synapta-node: {message}");
            ExitCode::FAILURE
        }
    }
}

fn keygen(out: &Path) -> Result<ExitCode, String> {
    if out.exists() {
        return Err(format!(
            "{} already exists; refusing to overwrite a private key",
            out.display()
        ));
    }
    if let Some(dir) = out.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let (private, public) = synapta_core::identity::generate_keypair();
    let public_path = out.with_extension("pub");
    write_secret(out, &private)?;
    std::fs::write(&public_path, &public).map_err(|e| format!("write {}: {e}", public_path.display()))?;
    println!("{public}");
    Ok(ExitCode::SUCCESS)
}

fn run(config_path: &Path) -> Result<ExitCode, String> {
    let config = NodeConfig::load(config_path)?;
    let data_dir = config.node.data_dir.clone();
    std::fs::create_dir_all(&data_dir).map_err(|e| format!("create {}: {e}", data_dir.display()))?;
    init_logging(&data_dir.join("node.log"))?;

    let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    runtime.block_on(serve(config, data_dir))
}

async fn serve(config: NodeConfig, data_dir: PathBuf) -> Result<ExitCode, String> {
    let api_listen = config.api.as_ref().map(|a| a.listen);
    let node = synapta_core::start(config).await?;

    let api = match api_listen {
        Some(listen) => {
            let token = synapta_gateway::generate_token();
            write_secret(&data_dir.join(TOKEN_FILE), &token)?;
            Some(synapta_gateway::start(node.clone(), listen, token).await?.addr)
        }
        None => None,
    };
    let info = RuntimeInfo {
        node_id: node.node_id().to_string(),
        listen: node.listen_addr(),
        api,
        pid: std::process::id(),
    };
    let runtime_file = data_dir.join(RUNTIME_FILE);
    std::fs::write(&runtime_file, serde_json::to_string_pretty(&info).unwrap_or_default())
        .map_err(|e| format!("write runtime.json: {e}"))?;
    eprintln!(
        "synapta-node: {} listening on {} (api: {:?})",
        info.node_id, info.listen, info.api
    );

    tokio::select! {
        _ = tokio::signal::ctrl_c() => tracing::info!("interrupt received"),
        _ = node.shutdown_requested() => tracing::info!("shutdown requested via local API"),
    }
    let clean = node.shutdown().await;
    let _ = std::fs::remove_file(&runtime_file);
    let _ = std::fs::remove_file(data_dir.join(TOKEN_FILE));
    Ok(if clean { ExitCode::SUCCESS } else { ExitCode::FAILURE })
}

/// Writes a private key or token readable by the owner only. On Unix the mode is 0600; on
/// Windows the file inherits the ACL of its folder (the user's profile / temp dir by default).
fn write_secret(path: &Path, contents: &str) -> Result<(), String> {
    use std::io::Write;
    let mut options = File::options();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options
        .open(path)
        .map_err(|e| format!("write {}: {e}", path.display()))?;
    file.write_all(contents.as_bytes())
        .map_err(|e| format!("write {}: {e}", path.display()))
}

/// Human-readable lines on stderr, JSON lines in `node.log`. `RUST_LOG` overrides the level.
fn init_logging(log_file: &Path) -> Result<(), String> {
    let file = File::options()
        .create(true)
        .append(true)
        .open(log_file)
        .map_err(|e| format!("open {}: {e}", log_file.display()))?;
    let filter = || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(filter()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(Mutex::new(file))
                .with_filter(filter()),
        )
        .try_init()
        .map_err(|e| e.to_string())
}
