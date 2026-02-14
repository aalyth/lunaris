use std::process;

use comfy_table::{ContentArrangement, Table};
use rustyline::error::ReadlineError;
use tokio::net::TcpStream;

use lunaris_common::protocol::{self, Request, Response};
use lunaris_common::value::Value;

const DEFAULT_SERVER_ADDR: &str = "127.0.0.1:7435";

const SERVER_ADDR_ENV_VAR: &str = "SERVER_ADDR";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server_addr = std::env::var(SERVER_ADDR_ENV_VAR).unwrap_or(DEFAULT_SERVER_ADDR.to_string());

    let stream = match TcpStream::connect(server_addr).await {
        Ok(s) => s,
        Err(err) => {
            eprintln!("Failed to connect to {DEFAULT_SERVER_ADDR}: {err}");
            eprintln!("Is the server running? Start it with: cargo run -p server");
            process::exit(1);
        }
    };

    let (mut reader, mut writer) = stream.into_split();

    println!("Connected to Lunaris at {DEFAULT_SERVER_ADDR}");
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

        let request = Request {
            sql: trimmed.to_string(),
        };
        if let Err(err) = protocol::send_message(&mut writer, &request).await {
            eprintln!("Send error: {err}");
            break;
        }

        match protocol::recv_message::<Response, _>(&mut reader).await {
            Ok(Some(Response::Ok(result))) => {
                if let Some(rs) = result.result_set {
                    print_result_set(&rs.columns, &rs.rows);
                }
                println!("{}", result.message);
            }
            Ok(Some(Response::Error { message })) => {
                eprintln!("Error: {message}");
            }
            Ok(None) => {
                eprintln!("Server closed connection.");
                break;
            }
            Err(e) => {
                eprintln!("Receive error: {e}");
                break;
            }
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
