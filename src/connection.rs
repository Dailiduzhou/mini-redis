use crate::frame::{self, Frame};

use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// Send and receive `Frame` values from a remote peer.
///
/// When implementing networking protocols, a message on that protocol is
/// often composed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
///
/// To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
#[derive(Debug)]
pub struct Connection {
    // The `TcpStream`. It is decorated with a `BufWriter`, which provides write
    // level buffering. The `BufWriter` implementation provided by Tokio is
    // sufficient for our needs.
    stream: BufWriter<TcpStream>,

    // The buffer for reading frames.
    buffer: BytesMut,
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`. Read and write buffers
    /// are initialized.
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // Default to a 4KB read buffer. For the use case of mini redis,
            // this is fine. However, real applications will want to tune this
            // value to their specific use case. There is a high likelihood that
            // a larger read buffer will work better.
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&self.buffer[..]);

        // The first step is to check if enough data has been buffered to parse
        // a single frame. This step is usually much faster than doing a full
        // parse of the frame, and allows us to skip allocating data structures
        // to hold the frame data unless we know the full frame has been
        // received.
        match Frame::check(&mut buf) {
            Ok(_) => {
                // The `check` function will have advanced the cursor until the
                // end of the frame. Since the cursor had position set to zero
                // before `Frame::check` was called, we obtain the length of the
                // frame by checking the cursor position.
                let len = buf.position() as usize;

                // Reset the position to zero before passing the cursor to
                // `Frame::parse`.
                buf.set_position(0);

                // Parse the frame from the buffer. This allocates the necessary
                // structures to represent the frame and returns the frame
                // value.
                //
                // If the encoded frame representation is invalid, an error is
                // returned. This should terminate the **current** connection
                // but should not impact any other connected client.
                let frame = Frame::parse(&mut buf)?;

                // Discard the parsed data from the read buffer.
                //
                // When `advance` is called on the read buffer, all of the data
                // up to `len` is discarded. The details of how this works is
                // left to `BytesMut`. This is often done by moving an internal
                // cursor, but it may be done by reallocating and copying data.
                self.buffer.advance(len);

                // Return the parsed frame to the caller.
                Ok(Some(frame))
            }
            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(Incomplete) => Ok(None),
            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(e) => Err(e.into()),
        }
    }

    /// Write a single `Frame` value to the underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        self.write_value(frame).await?;

        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.stream.flush().await
    }

    /// Write a frame to the stream.
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        Self::write_value_to(&mut self.stream, frame).await
    }

    async fn write_value_to<W>(stream: &mut W, frame: &Frame) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let mut stack = vec![frame];

        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Simple(val) => {
                    stream.write_u8(b'+').await?;
                    stream.write_all(val.as_bytes()).await?;
                    stream.write_all(b"\r\n").await?;
                }
                Frame::Error(val) => {
                    stream.write_u8(b'-').await?;
                    stream.write_all(val.as_bytes()).await?;
                    stream.write_all(b"\r\n").await?;
                }
                Frame::Integer(val) => {
                    stream.write_u8(b':').await?;
                    Self::write_decimal_to(stream, *val).await?;
                }
                Frame::Null => {
                    stream.write_all(b"$-1\r\n").await?;
                }
                Frame::Bulk(val) => {
                    stream.write_u8(b'$').await?;
                    Self::write_decimal_to(stream, val.len() as u64).await?;
                    stream.write_all(val).await?;
                    stream.write_all(b"\r\n").await?;
                }
                Frame::Array(values) => {
                    stream.write_u8(b'*').await?;
                    Self::write_decimal_to(stream, values.len() as u64).await?;

                    for value in values.iter().rev() {
                        stack.push(value);
                    }
                }
            }
        }

        Ok(())
    }

    /// Write a decimal frame to the stream
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        Self::write_decimal_to(&mut self.stream, val).await
    }

    async fn write_decimal_to<W>(stream: &mut W, val: u64) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        use std::io::Write;

        // Convert the value to a string
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{val}")?;

        let pos = buf.position() as usize;
        stream.write_all(&buf.get_ref()[..pos]).await?;
        stream.write_all(b"\r\n").await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::io::{duplex, AsyncReadExt};

    #[tokio::test]
    async fn writes_empty_array_frame() {
        let frame = Frame::Array(vec![]);

        assert_encoded(frame, b"*0\r\n").await;
    }

    #[tokio::test]
    async fn writes_flat_array_frame() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"SET")),
            Frame::Bulk(Bytes::from_static(b"hello")),
            Frame::Bulk(Bytes::from_static(b"world")),
        ]);

        assert_encoded(frame, b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n").await;
    }

    #[tokio::test]
    async fn writes_nested_array_frame() {
        let frame = Frame::Array(vec![
            Frame::Simple("outer".into()),
            Frame::Array(vec![Frame::Integer(1), Frame::Null]),
            Frame::Bulk(Bytes::from_static(b"tail")),
        ]);

        assert_encoded(frame, b"*3\r\n+outer\r\n*2\r\n:1\r\n$-1\r\n$4\r\ntail\r\n").await;
    }

    async fn assert_encoded(frame: Frame, expected: &[u8]) {
        let (stream, mut peer) = duplex(64);
        let mut writer = BufWriter::new(stream);

        Connection::write_value_to(&mut writer, &frame)
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let mut actual = vec![0; expected.len()];
        peer.read_exact(&mut actual).await.unwrap();

        assert_eq!(actual, expected);
    }
}
