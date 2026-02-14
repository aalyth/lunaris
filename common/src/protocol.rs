use crate::value::Value;
use std::io;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub sql: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok(QueryResult),
    Error { message: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub message: String,
    pub result_set: Option<ResultSet>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

pub async fn send_message<T: Serialize, W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg: &T,
) -> io::Result<()> {
    let payload =
        serde_json::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len = payload.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

/// Returns `None` on clean EOF (connection closed).
pub async fn recv_message<T: for<'de> Deserialize<'de>, R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> io::Result<Option<T>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;

    let msg = serde_json::from_slice(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(msg))
}
