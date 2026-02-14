use std::sync::Arc;

use tokio::net::TcpStream;

use crate::database::Database;
use lunaris_common::protocol::{self, QueryResult, Request, Response, ResultSet};

pub async fn handle_connection(stream: TcpStream, db: Arc<Database>) {
    let addr = stream.peer_addr().ok();
    if let Some(a) = &addr {
        eprintln!("[server] client connected: {a}");
    }

    let (mut reader, mut writer) = stream.into_split();

    loop {
        let request: Option<Request> = match protocol::recv_message(&mut reader).await {
            Ok(Some(req)) => Some(req),
            Ok(None) => {
                if let Some(a) = &addr {
                    eprintln!("[server] client disconnected: {a}");
                }
                return;
            }
            Err(e) => {
                eprintln!("[server] read error: {e}");
                return;
            }
        };

        let request = match request {
            Some(r) => r,
            None => return,
        };

        let response = match db.execute_sql(&request.sql) {
            Ok(result) => {
                let result_set = if !result.rows.is_empty() {
                    Some(ResultSet {
                        columns: result.columns,
                        rows: result.rows,
                    })
                } else {
                    None
                };

                Response::Ok(QueryResult {
                    message: result.message,
                    result_set,
                })
            }
            Err(e) => Response::Error {
                message: e.to_string(),
            },
        };

        if let Err(e) = protocol::send_message(&mut writer, &response).await {
            eprintln!("[server] write error: {e}");
            return;
        }
    }
}
