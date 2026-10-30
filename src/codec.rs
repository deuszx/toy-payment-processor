use std::io;

use tokio::io::AsyncRead;
use tokio_stream::{Stream, StreamExt};
use tokio_util::{
    bytes::BytesMut,
    codec::{Decoder, FramedRead},
};

use crate::{
    amount::{AmountError, parse_amount},
    transaction::Transaction,
};

/// Why a row could not become a `Transaction`.
#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    InvalidUtf8,
    UnknownType(String),
    MissingField(&'static str),
    InvalidClient(String),
    InvalidTx(String),
    InvalidAmount(AmountError),
    ExtraField(String),
}

/// Why `csv_transactions` could not produce a transaction.
#[derive(Debug)]
pub enum InputError {
    /// The row is bad. Later rows are unaffected.
    Parse(ParseError),
    /// Reading failed, and the stream ends here.
    Io(io::Error),
}

/// Parses one row. Columns are `type, client, tx, amount`, and only deposits and
/// withdrawals have an amount.
pub fn parse_line(line: &str) -> Result<Transaction, ParseError> {
    let mut fields = line.split(',').map(str::trim);
    let transaction = match fields.next().unwrap_or_default() {
        "deposit" => {
            let (client, tx) = parse_ids(&mut fields)?;
            let amount = parse_amount_field(&mut fields)?;
            Transaction::Deposit { client, tx, amount }
        }
        "withdrawal" => {
            let (client, tx) = parse_ids(&mut fields)?;
            let amount = parse_amount_field(&mut fields)?;
            Transaction::Withdrawal { client, tx, amount }
        }
        "dispute" => {
            let (client, tx) = parse_ids(&mut fields)?;
            Transaction::Dispute { client, tx }
        }
        "resolve" => {
            let (client, tx) = parse_ids(&mut fields)?;
            Transaction::Resolve { client, tx }
        }
        "chargeback" => {
            let (client, tx) = parse_ids(&mut fields)?;
            Transaction::Chargeback { client, tx }
        }
        other => return Err(ParseError::UnknownType(other.to_string())),
    };
    ensure_no_more_fields(&mut fields)?;
    Ok(transaction)
}

fn parse_ids<'a>(fields: &mut impl Iterator<Item = &'a str>) -> Result<(u16, u32), ParseError> {
    let client = required(fields, "client")?;
    let client = client
        .parse()
        .map_err(|_| ParseError::InvalidClient(client.to_string()))?;
    let tx = required(fields, "tx")?;
    let tx = tx
        .parse()
        .map_err(|_| ParseError::InvalidTx(tx.to_string()))?;
    Ok((client, tx))
}

fn parse_amount_field<'a>(fields: &mut impl Iterator<Item = &'a str>) -> Result<u32, ParseError> {
    parse_amount(required(fields, "amount")?).map_err(ParseError::InvalidAmount)
}

/// Refuses anything past the last column, because `1,000.50` would otherwise apply as 1.
fn ensure_no_more_fields<'a>(fields: &mut impl Iterator<Item = &'a str>) -> Result<(), ParseError> {
    match fields.find(|field| !field.is_empty()) {
        Some(extra) => Err(ParseError::ExtraField(extra.to_string())),
        None => Ok(()),
    }
}

fn required<'a>(
    fields: &mut impl Iterator<Item = &'a str>,
    name: &'static str,
) -> Result<&'a str, ParseError> {
    fields
        .next()
        .filter(|field| !field.is_empty())
        .ok_or(ParseError::MissingField(name))
}

/// Decodes CSV rows, skipping the header line and blank lines.
///
/// `\n`, `\r\n` and `\r` all end a line. `\r\n` counts as a line ending plus a blank
/// line, even when split across reads.
///
/// A bad row is an `Item`, never a `Decoder::Error`, because `FramedRead` ends the
/// stream after any decoder error.
#[derive(Default)]
pub struct TransactionCodec {
    header_skipped: bool,
}

impl TransactionCodec {
    /// Returns the line's row, or `None` for the header and blank lines.
    fn decode_line(&mut self, line: &[u8]) -> Option<Result<Transaction, ParseError>> {
        if !self.header_skipped {
            self.header_skipped = true;
            return None;
        }
        match std::str::from_utf8(line) {
            Ok(text) if text.trim().is_empty() => None,
            Ok(text) => Some(parse_line(text)),
            Err(_) => Some(Err(ParseError::InvalidUtf8)),
        }
    }
}

impl Decoder for TransactionCodec {
    type Item = Result<Transaction, ParseError>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        // Loops because returning `None` here would stall complete lines still in `src`.
        while let Some(newline) = src.iter().position(|byte| matches!(*byte, b'\n' | b'\r')) {
            let line = src.split_to(newline + 1);
            if let Some(row) = self.decode_line(&line) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, io::Error> {
        if let Some(row) = self.decode(src)? {
            return Ok(Some(row));
        }
        if src.is_empty() {
            return Ok(None);
        }
        let line = src.split();
        Ok(self.decode_line(&line))
    }
}

/// Turns CSV bytes from any reader into a stream of transactions.
pub fn csv_transactions<R: AsyncRead>(
    reader: R,
) -> impl Stream<Item = Result<Transaction, InputError>> {
    FramedRead::new(reader, TransactionCodec::default()).map(|row| match row {
        Ok(Ok(transaction)) => Ok(transaction),
        Ok(Err(error)) => Err(InputError::Parse(error)),
        Err(error) => Err(InputError::Io(error)),
    })
}

#[cfg(test)]
mod tests {
    use tokio_stream::StreamExt;
    use tokio_util::{bytes::BytesMut, codec::Decoder};

    use super::*;

    #[test]
    fn parses_padded_and_short_rows() {
        let d = Transaction::Deposit {
            client: 1,
            tx: 2,
            amount: 10_000,
        };
        assert_eq!(parse_line("deposit,1,2,1.0"), Ok(d.clone()));
        assert_eq!(parse_line("  deposit , 1, 2, 1.0 "), Ok(d));
        let w = Transaction::Withdrawal {
            client: 1,
            tx: 2,
            amount: 5_000,
        };
        assert_eq!(parse_line("withdrawal,1,2,0.5"), Ok(w));
        let dispute = Transaction::Dispute { client: 1, tx: 2 };
        for row in ["dispute,1,2", "dispute,1,2,"] {
            assert_eq!(parse_line(row), Ok(dispute.clone()), "{row:?}");
        }
        assert_eq!(
            parse_line("resolve,1,2"),
            Ok(Transaction::Resolve { client: 1, tx: 2 })
        );
        assert_eq!(
            parse_line("chargeback,1,2"),
            Ok(Transaction::Chargeback { client: 1, tx: 2 })
        );
    }

    #[test]
    fn rejects_bad_rows() {
        assert_eq!(
            parse_line("refund,1,1,1.0"),
            Err(ParseError::UnknownType("refund".into()))
        );
        assert_eq!(
            parse_line("Deposit,1,1,1.0"),
            Err(ParseError::UnknownType("Deposit".into()))
        );
        assert_eq!(
            parse_line("deposit"),
            Err(ParseError::MissingField("client"))
        );
        assert_eq!(parse_line("deposit,1"), Err(ParseError::MissingField("tx")));
        assert_eq!(
            parse_line("deposit,1,1"),
            Err(ParseError::MissingField("amount"))
        );
        assert_eq!(
            parse_line("deposit,1,1,"),
            Err(ParseError::MissingField("amount"))
        );
        assert_eq!(
            parse_line("deposit,70000,1,1.0"),
            Err(ParseError::InvalidClient("70000".into()))
        );
        assert_eq!(
            parse_line("deposit,1,-1,1.0"),
            Err(ParseError::InvalidTx("-1".into()))
        );
        assert_eq!(
            parse_line("deposit,1,1,1.23456"),
            Err(ParseError::InvalidAmount(AmountError::TooManyDecimals))
        );
    }

    #[test]
    fn rejects_extra_columns_instead_of_applying_a_misread_amount() {
        assert_eq!(
            parse_line("deposit,1,1,1,000.50"),
            Err(ParseError::ExtraField("000.50".into()))
        );
        assert_eq!(
            parse_line("withdrawal,1,1,2,5"),
            Err(ParseError::ExtraField("5".into()))
        );
        assert_eq!(
            parse_line("dispute,1,1,,extra"),
            Err(ParseError::ExtraField("extra".into()))
        );
    }

    #[test]
    fn rejects_amount_on_rows_that_take_none() {
        for row in ["dispute,1,1,5.0", "resolve,1,1,5.0", "chargeback,1,1,5.0"] {
            assert_eq!(
                parse_line(row),
                Err(ParseError::ExtraField("5.0".into())),
                "{row:?}"
            );
        }
    }

    #[test]
    fn tolerates_trailing_blank_columns() {
        let d = Transaction::Deposit {
            client: 1,
            tx: 2,
            amount: 10_000,
        };
        assert_eq!(parse_line("deposit,1,2,1.0,,"), Ok(d));
        let dispute = Transaction::Dispute { client: 1, tx: 2 };
        assert_eq!(parse_line("dispute,1,2,,"), Ok(dispute));
    }

    #[tokio::test]
    async fn cr_only_line_endings_separate_rows() {
        let input = &b"type,client,tx,amount\rdeposit,1,1,1.0\rdeposit,1,2,1.0\r"[..];
        let rows: Vec<_> = csv_transactions(input).collect().await;
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(Result::is_ok));
    }

    #[test]
    fn crlf_split_across_reads_is_one_line_ending() {
        let mut codec = TransactionCodec::default();
        let mut buf = BytesMut::from(&b"type,client,tx,amount\r\ndeposit,1,1,1.0\r"[..]);
        assert_eq!(
            codec.decode(&mut buf).unwrap(),
            Some(Ok(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: 10_000
            }))
        );
        buf.extend_from_slice(b"\ndeposit,1,2,1.0\r\n");
        assert_eq!(
            codec.decode(&mut buf).unwrap(),
            Some(Ok(Transaction::Deposit {
                client: 1,
                tx: 2,
                amount: 10_000
            }))
        );
    }

    #[tokio::test]
    async fn skips_header_and_blank_lines_and_handles_crlf_and_missing_final_newline() {
        let input = &b"type,client,tx,amount\r\ndeposit,1,1,1.0\r\n\r\nwithdrawal,1,2,0.5"[..];
        let rows: Vec<_> = csv_transactions(input).collect().await;
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(Result::is_ok));
    }

    #[tokio::test]
    async fn bad_rows_do_not_stop_the_stream() {
        let input =
            &b"type,client,tx,amount\ndeposit,1,1,1.0\nnonsense\n\xFF\xFE,1,2\ndeposit,1,3,1.0\n"[..];
        let rows: Vec<_> = csv_transactions(input).collect().await;
        assert_eq!(rows.len(), 4);
        assert!(rows[0].is_ok() && rows[3].is_ok());
        assert!(matches!(
            rows[1],
            Err(InputError::Parse(ParseError::UnknownType(_)))
        ));
        assert!(matches!(
            rows[2],
            Err(InputError::Parse(ParseError::InvalidUtf8))
        ));
    }

    #[test]
    fn row_split_across_reads_decodes_once_complete() {
        let mut codec = TransactionCodec::default();
        let mut buf = BytesMut::from(&b"type,client,tx,amount\ndeposit,1,"[..]);
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        buf.extend_from_slice(b"1,1.0\n");
        assert_eq!(
            codec.decode(&mut buf).unwrap(),
            Some(Ok(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: 10_000
            }))
        );
    }

    #[tokio::test]
    async fn empty_input_yields_nothing() {
        assert_eq!(
            csv_transactions(&b""[..]).collect::<Vec<_>>().await.len(),
            0
        );
    }
}
