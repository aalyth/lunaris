use std::process;

use comfy_table::{ContentArrangement, Table};
use rustyline::error::ReadlineError;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use lunaris_common::protocol::{self, Request, Response};
use lunaris_common::value::Value;

const DEFAULT_SERVER_ADDR: &str = "127.0.0.1:7435";

const SERVER_ADDR_ENV_VAR: &str = "SERVER_ADDR";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server_addr = std::env::var(SERVER_ADDR_ENV_VAR).unwrap_or(DEFAULT_SERVER_ADDR.to_string());

    let stream = match TcpStream::connect(&server_addr).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("Failed to connect to {server_addr}: {err}");
            eprintln!("Is the server running? Start it with: cargo run -p server");
            process::exit(1);
        }
    };

    let (mut reader, mut writer) = stream.into_split();

    if let Some(path) = std::env::args().nth(1) {
        run_script(&path, &mut reader, &mut writer).await
    } else {
        run_repl(&server_addr, &mut reader, &mut writer).await
    }
}

async fn run_script(
    path: &str,
    reader: &mut OwnedReadHalf,
    writer: &mut OwnedWriteHalf,
) -> anyhow::Result<()> {
    let contents = std::fs::read_to_string(path)?;

    for stmt in contents.split(';') {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Err(e) = send_and_display(trimmed, reader, writer).await {
            eprintln!("Error: {e}");
            process::exit(1);
        }
    }

    Ok(())
}

async fn run_repl(
    server_addr: &str,
    reader: &mut OwnedReadHalf,
    writer: &mut OwnedWriteHalf,
) -> anyhow::Result<()> {
    println!("Connected to Lunaris at {server_addr}");
    println!("Type SQL statements, or 'exit' to quit.\n");

    let mut rl = rustyline::DefaultEditor::new()?;
    loop {
        let line = match rl.readline("lunaris> ") {
            Ok(line) => line,
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                println!("Goodbye.");
                break;
            }
            Err(e) => {
                eprintln!("Input error: {e}");
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.eq_ignore_ascii_case("exit") || trimmed.eq_ignore_ascii_case("quit") {
            println!("Goodbye.");
            break;
        }

        rl.add_history_entry(&line)?;

        if let Err(e) = send_and_display(trimmed, reader, writer).await {
            eprintln!("Error: {e}");
            break;
        }
    }

    Ok(())
}

async fn send_and_display(
    sql: &str,
    reader: &mut OwnedReadHalf,
    writer: &mut OwnedWriteHalf,
) -> anyhow::Result<()> {
    let request = Request {
        sql: sql.to_string(),
    };
    protocol::send_message(writer, &request).await?;

    match protocol::recv_message::<Response, _>(reader).await? {
        Some(Response::Ok(result)) => {
            if let Some(rs) = result.result_set {
                print_result_set(&rs.columns, &rs.rows);
            }
            println!("{}", result.message);
        }
        Some(Response::Error { message }) => {
            eprintln!("Error: {message}");
        }
        None => {
            anyhow::bail!("Server closed connection.");
        }
    }

    Ok(())
}

fn print_result_set(columns: &[String], rows: &[Vec<Value>]) {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(columns);

    for row in rows {
        let cells: Vec<String> = row.iter().map(format_value).collect();
        table.add_row(cells);
    }

    println!("{table}");
}

fn format_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Text(s) => s.clone(),
    }
}
