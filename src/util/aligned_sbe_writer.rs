//! [`AlignedSbeWriter`] writes SBE messages in fixed-size, zero-padded chunks.

use std::{fs::File, io::Write, path::Path};

use super::dtos::SbeEncode;

// Every schema in `dtos` uses the standard eight-byte SBE message header.
const HEADER_LENGTH: usize = 8;

/// Writes SBE messages to a flat file in fixed size, zero padded chunks.
///
/// See [`AlignedSbeReader`](crate::util::AlignedSbeReader) for more details on why
/// this struct exists.
///
/// Every chunk reserves enough trailing zeroes for an empty SBE header, so a
/// reader can stop decoding at the sentinel without carrying a frame across a
/// chunk boundary.
///
/// The writer is generic over any [`Write`], so the same chunking logic can
/// target a file ([`Self::create`]) or a stream such as `stdout`
/// ([`Self::from_writer`]).
///
/// Call [`Self::flush`] to write the final buffered chunk before dropping the
/// writer.
pub struct AlignedSbeWriter<W: std::io::Write = std::fs::File> {
    writer: W,
    buf: Vec<u8>,
    chunk: usize,
}

impl AlignedSbeWriter<File> {
    /// Create an [`AlignedSbeWriter`], writing to `path` with `chunk` chunk
    /// size. Clears the file at `path`, if it exists.
    pub fn create(path: impl AsRef<Path>, chunk: usize) -> anyhow::Result<Self> {
        Self::from_writer(File::create(path)?, chunk)
    }
}

impl<W: Write> AlignedSbeWriter<W> {
    /// Create an [`AlignedSbeWriter`] that writes chunks to an already-open
    /// `writer`, with `chunk` chunk size. Use this to stream SBE output to
    /// `stdout` or any other [`Write`] implementation.
    pub fn from_writer(writer: W, chunk: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            chunk > HEADER_LENGTH,
            "chunk must leave room for SBE data and an SBE header"
        );
        Ok(Self {
            writer,
            buf: Vec::with_capacity(chunk),
            chunk,
        })
    }

    /// Encodes a complete message directly into the chunk buffer. Rotates to a
    /// new chunk if it does not fit in the existing chunk.
    pub fn write_msg<T: SbeEncode + ?Sized>(&mut self, message: &T) -> anyhow::Result<()> {
        let len = message.encoded_length_with_header()?;
        let payload_limit = self.chunk - HEADER_LENGTH;
        anyhow::ensure!(len <= payload_limit, "SBE message does not fit in a chunk");
        if len > payload_limit - self.buf.len() {
            self.flush()?;
        }

        let start = self.buf.len();
        self.buf.resize(start + len, 0);
        let result = message.encode(&mut self.buf[start..]).and_then(|written| {
            anyhow::ensure!(
                written == len,
                "SBE encoded length mismatch: expected {len} bytes, wrote {written}"
            );
            Ok(())
        });
        if result.is_err() {
            self.buf.truncate(start);
        }
        result
    }

    /// Flush the current buffer to the underlying writer, and clear it.
    pub fn flush(&mut self) -> anyhow::Result<()> {
        if !self.buf.is_empty() {
            self.buf.resize(self.chunk, 0);
            self.writer.write_all(&self.buf)?;
            self.buf.clear();
        }
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal message whose payload is `PAYLOAD` bytes of `0xAA`.
    struct FixedMessage;

    const PAYLOAD: usize = 4;

    impl SbeEncode for FixedMessage {
        fn encoded_length_with_header(&self) -> anyhow::Result<usize> {
            Ok(HEADER_LENGTH + PAYLOAD)
        }

        fn encode(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
            buf.fill(0xAA);
            Ok(buf.len())
        }
    }

    #[test]
    fn generic_writer_pads_chunks_and_flushes() -> anyhow::Result<()> {
        let message_len = HEADER_LENGTH + PAYLOAD;
        let chunk = message_len + HEADER_LENGTH;

        let mut writer = AlignedSbeWriter::from_writer(Vec::new(), chunk)?;
        writer.write_msg(&FixedMessage)?;
        writer.write_msg(&FixedMessage)?;
        writer.flush()?;

        let bytes = writer.writer;
        assert_eq!(bytes.len(), chunk * 2);
        assert!(bytes[..message_len].iter().all(|&byte| byte == 0xAA));
        assert!(bytes[message_len..chunk].iter().all(|&byte| byte == 0));
        assert!(
            bytes[chunk..chunk + message_len]
                .iter()
                .all(|&byte| byte == 0xAA)
        );
        assert!(bytes[chunk + message_len..].iter().all(|&byte| byte == 0));

        Ok(())
    }

    #[test]
    fn chunk_must_leave_room_for_header() {
        assert!(AlignedSbeWriter::from_writer(Vec::new(), HEADER_LENGTH).is_err());
        assert!(AlignedSbeWriter::from_writer(Vec::new(), HEADER_LENGTH + 1).is_ok());
    }
}
