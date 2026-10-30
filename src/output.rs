use std::io;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    amount::{format_amount, format_difference},
    client::ClientSnapshot,
};

const HEADER: &str = "client,available,held,total,locked\n";

/// Reports client accounts in some output format.
pub trait ClientWriter {
    /// Writes one client.
    fn write(&mut self, client: &ClientSnapshot) -> impl Future<Output = io::Result<()>> + Send;

    /// Completes the report. Call once, after the last client.
    fn finish(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}

/// Writes clients as CSV, with amounts to exactly 4 decimals.
pub struct CsvClientWriter<W> {
    writer: W,
    header_written: bool,
}

impl<W> CsvClientWriter<W> {
    pub fn new(writer: W) -> Self {
        CsvClientWriter {
            writer,
            header_written: false,
        }
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: AsyncWrite + Unpin + Send> CsvClientWriter<W> {
    async fn ensure_header(&mut self) -> io::Result<()> {
        if !self.header_written {
            self.writer.write_all(HEADER.as_bytes()).await?;
            self.header_written = true;
        }
        Ok(())
    }
}

impl<W: AsyncWrite + Unpin + Send> ClientWriter for CsvClientWriter<W> {
    async fn write(&mut self, client: &ClientSnapshot) -> io::Result<()> {
        self.ensure_header().await?;
        let row = format!(
            "{},{},{},{},{}\n",
            client.id,
            format_difference(client.total, client.held),
            format_amount(client.held),
            format_amount(client.total),
            client.locked
        );
        self.writer.write_all(row.as_bytes()).await
    }

    async fn finish(&mut self) -> io::Result<()> {
        self.ensure_header().await?;
        self.writer.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &str = "client,available,held,total,locked\n";

    #[tokio::test]
    async fn no_clients_still_writes_header() {
        let mut w = CsvClientWriter::new(Vec::new());
        w.finish().await.unwrap();
        assert_eq!(w.into_inner(), HEADER.as_bytes());
    }

    #[tokio::test]
    async fn writes_four_decimals_and_negative_available() {
        let mut w = CsvClientWriter::new(Vec::new());
        w.write(&ClientSnapshot {
            id: 1,
            total: 15_000,
            held: 0,
            locked: false,
        })
        .await
        .unwrap();
        w.write(&ClientSnapshot {
            id: 2,
            total: 200_000,
            held: 1_000_000,
            locked: true,
        })
        .await
        .unwrap();
        w.finish().await.unwrap();
        let expected =
            format!("{HEADER}1,1.5000,0.0000,1.5000,false\n2,-80.0000,100.0000,20.0000,true\n");
        assert_eq!(String::from_utf8(w.into_inner()).unwrap(), expected);
    }
}
