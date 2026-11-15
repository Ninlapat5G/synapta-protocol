//! `synapta-lab` — opens the Synapta Lab web page.
//!
//! ```text
//! synapta-lab [--port 7878] [--listen <ip>]... [--no-tailscale]
//! ```
//! Listens on 127.0.0.1 and, when Tailscale is up, this machine's Tailscale address. `--listen`
//! replaces that list; only loopback and Tailscale addresses are accepted (`listen.rs`).
//! The nodes themselves only ever use loopback addresses (127.1.x.y).

use std::net::{IpAddr, SocketAddr};
use std::process::ExitCode;
use std::sync::Arc;

use synapta_lab::api::Api;
use synapta_lab::{http, listen, Lab};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// Hundreds of nodes in one process allocate and free millions of small objects a second (frames,
/// trace events) from every thread. Windows' default heap serialised that: a ring of 100 never
/// finished connecting, with most runtime threads waiting on the heap lock (measured). mimalloc
/// keeps per-thread heaps; with it the same ring connects in about a second.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DEFAULT_PORT: u16 = 7878;
const USAGE: &str = "usage: synapta-lab [--port 7878] [--listen <ip>]... [--no-tailscale]";

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn,synapta_lab=info".into()),
        )
        .init();
    let addresses = match addresses(std::env::args().skip(1).collect()) {
        Ok(addresses) => addresses,
        Err(message) => {
            eprintln!("synapta-lab: {message}\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    match runtime.block_on(run(addresses)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("synapta-lab: {message}");
            ExitCode::FAILURE
        }
    }
}

/// Reads the command line into the addresses to listen on, every one checked.
fn addresses(mut args: Vec<String>) -> Result<Vec<SocketAddr>, String> {
    let mut take_value = |flag: &str| -> Result<Vec<String>, String> {
        let mut values = Vec::new();
        while let Some(i) = args.iter().position(|a| a == flag) {
            args.remove(i);
            if i >= args.len() {
                return Err(format!("{flag} needs a value"));
            }
            values.push(args.remove(i));
        }
        Ok(values)
    };
    let port = match take_value("--port")?.pop() {
        Some(p) => p.parse().map_err(|_| format!("bad port {p:?}"))?,
        None => DEFAULT_PORT,
    };
    let explicit: Vec<IpAddr> = take_value("--listen")?
        .iter()
        .map(|ip| ip.parse().map_err(|_| format!("bad address {ip:?}")))
        .collect::<Result<_, _>>()?;
    let no_tailscale = args.iter().any(|a| a == "--no-tailscale");
    args.retain(|a| a != "--no-tailscale");
    if !args.is_empty() {
        return Err(format!("unknown arguments {args:?}"));
    }

    let ips = if !explicit.is_empty() {
        explicit
    } else {
        let mut ips = vec![listen::LOOPBACK];
        match (no_tailscale, listen::tailscale_ip()) {
            (true, _) => {}
            (false, Some(ip)) => ips.push(ip),
            (false, None) => eprintln!("synapta-lab: Tailscale not found or not up; loopback only"),
        }
        ips
    };
    for ip in &ips {
        listen::check(*ip)?;
    }
    Ok(ips.into_iter().map(|ip| SocketAddr::new(ip, port)).collect())
}

async fn run(addresses: Vec<SocketAddr>) -> Result<(), String> {
    let lab = Arc::new(Lab::new().map_err(|e| format!("temp dir: {e}"))?);
    let api = Api::new(lab.clone());
    let shutdown = CancellationToken::new();
    let mut servers = Vec::new();
    for address in &addresses {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|e| format!("listen on {address}: {e}"))?;
        println!("Synapta Lab: http://{address}/");
        servers.push(tokio::spawn(http::serve(listener, api.clone(), shutdown.clone())));
    }
    tokio::signal::ctrl_c().await.map_err(|e| e.to_string())?;
    println!("stopping...");
    shutdown.cancel();
    for server in servers {
        let _ = server.await;
    }
    lab.shutdown().await;
    Ok(())
}
