use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;

use lunaris_server::database::Database;
use lunaris_server::server;

const DEFAULT_PORT: u16 = 7435;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_dir = std::env::var("LUNARIS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| dirs_home().join(".lunaris"));
    eprintln!("[server] data directory: {}", db_dir.display());

    let db = Database::open(db_dir)?;
    let db = Arc::new(db);

    let addr = format!("127.0.0.1:{DEFAULT_PORT}");
    let listener = TcpListener::bind(&addr).await?;
    eprintln!("[server] listening on {addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            server::handle_connection(stream, db).await;
        });
    }
}

fn dirs_home() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}
