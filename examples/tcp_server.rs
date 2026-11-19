// You can run this example from the root of the mio repo:
// cargo run --example tcp_server --features="os-poll net"
use mio::event::Event;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Registry, Token};
use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::str::from_utf8;

// Setup some tokens to allow us to identify which event is for which socket.
const SERVER: Token = Token(0);

// Some data we'll send over the connection.
const DATA: &[u8] = b"Hello world!\n";

/// A connection and its send queue.
///
/// Mio reports readiness edge-triggered: it tells us when a socket *becomes*
/// writable, not that it *is* writable. A single `write` may accept only part
/// of a message, so the connection itself must remember the unsent remainder
/// and resume sending on the next writable event.
struct Connection {
    stream: TcpStream,
    /// Bytes accepted from the application but not yet written to the socket.
    outbox: VecDeque<u8>,
}

impl Connection {
    fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            outbox: VecDeque::new(),
        }
    }

    /// Queue `data` for sending. The actual write happens in `flush_outbox`,
    /// either right away or when the socket becomes writable.
    fn queue_data(&mut self, data: &[u8]) {
        self.outbox.extend(data);
    }
}

#[cfg(not(target_os = "wasi"))]
fn main() -> io::Result<()> {
    env_logger::init();

    // Create a poll instance.
    let mut poll = Poll::new()?;
    // Create storage for events.
    let mut events = Events::with_capacity(128);

    // Setup the TCP server socket.
    let addr = "127.0.0.1:9000".parse().unwrap();
    let mut server = TcpListener::bind(addr)?;

    // Register the server with poll we can receive events for it.
    poll.registry()
        .register(&mut server, SERVER, Interest::READABLE)?;

    // Map of `Token` -> `Connection`.
    let mut connections = HashMap::new();
    // Unique token for each incoming connection.
    let mut unique_token = Token(SERVER.0 + 1);

    println!("You can connect to the server using `nc`:");
    println!(" $ nc 127.0.0.1 9000");
    println!("You'll see our welcome message and anything you type will be printed here.");

    loop {
        if let Err(err) = poll.poll(&mut events, None) {
            if interrupted(&err) {
                continue;
            }
            return Err(err);
        }

        for event in events.iter() {
            match event.token() {
                SERVER => loop {
                    // Received an event for the TCP server socket, which
                    // indicates we can accept an connection.
                    let (connection, address) = match server.accept() {
                        Ok((connection, address)) => (connection, address),
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            // If we get a `WouldBlock` error we know our
                            // listener has no more incoming connections queued,
                            // so we can return to polling and wait for some
                            // more.
                            break;
                        }
                        Err(e) => {
                            // If it was any other kind of error, something went
                            // wrong and we terminate with an error.
                            return Err(e);
                        }
                    };

                    println!("Accepted connection from: {address}");

                    let token = next(&mut unique_token);
                    let mut connection = Connection::new(connection);
                    // Greet the client. The send queue is non-empty, so we
                    // also listen for writable events until it is flushed.
                    connection.queue_data(DATA);
                    poll.registry().register(
                        &mut connection.stream,
                        token,
                        Interest::READABLE.add(Interest::WRITABLE),
                    )?;

                    connections.insert(token, connection);
                },
                token => {
                    // Maybe received an event for a TCP connection.
                    let done = if let Some(connection) = connections.get_mut(&token) {
                        handle_connection_event(poll.registry(), connection, event)?
                    } else {
                        // Sporadic events happen, we can safely ignore them.
                        false
                    };
                    if done {
                        if let Some(mut connection) = connections.remove(&token) {
                            poll.registry().deregister(&mut connection.stream)?;
                        }
                    }
                }
            }
        }
    }
}

fn next(current: &mut Token) -> Token {
    let next = current.0;
    current.0 += 1;
    Token(next)
}

/// Returns `true` if the connection is done.
fn handle_connection_event(
    registry: &Registry,
    connection: &mut Connection,
    event: &Event,
) -> io::Result<bool> {
    if event.is_writable() {
        // We can (maybe) write to the connection. Flush as much of the send
        // queue as the kernel accepts right now; whatever doesn't fit stays
        // queued and we'll resume on the next writable event.
        flush_outbox(connection)?;

        if connection.outbox.is_empty() {
            // The send queue is drained. Stop listening for writable events:
            // with nothing to send they'd only wake us up to do nothing,
            // spinning the CPU on an idle connection.
            registry.reregister(&mut connection.stream, event.token(), Interest::READABLE)?;
        }
    }

    if event.is_readable() {
        let mut connection_closed = false;
        let mut received_data = vec![0; 4096];
        let mut bytes_read = 0;
        // We can (maybe) read from the connection.
        loop {
            match connection.stream.read(&mut received_data[bytes_read..]) {
                Ok(0) => {
                    // Reading 0 bytes means the other side has closed the
                    // connection or is done writing, then so are we.
                    connection_closed = true;
                    break;
                }
                Ok(n) => {
                    bytes_read += n;
                    if bytes_read == received_data.len() {
                        received_data.resize(received_data.len() + 1024, 0);
                    }
                }
                // Would block "errors" are the OS's way of saying that the
                // connection is not actually ready to perform this I/O operation.
                Err(ref err) if would_block(err) => break,
                Err(ref err) if interrupted(err) => continue,
                // Other errors we'll consider fatal.
                Err(err) => return Err(err),
            }
        }

        if bytes_read != 0 {
            let received_data = &received_data[..bytes_read];
            if let Ok(str_buf) = from_utf8(received_data) {
                println!("Received data: {}", str_buf.trim_end());
            } else {
                println!("Received (none UTF-8) data: {received_data:?}");
            }

            // Echo the data back. It goes through the send queue like any
            // other outgoing message, so a slow client can't lose the tail
            // of it to a partial write.
            connection.queue_data(received_data);
            flush_outbox(connection)?;
            if !connection.outbox.is_empty() {
                // Couldn't send everything right now. Register for writable
                // events so we resume once the kernel buffer drains.
                registry.reregister(
                    &mut connection.stream,
                    event.token(),
                    Interest::READABLE.add(Interest::WRITABLE),
                )?;
            }
        }

        if connection_closed {
            println!("Connection closed");
            return Ok(true);
        }
    }

    Ok(false)
}

/// Write as much of the send queue as the socket accepts without blocking.
///
/// A short write is not an error: the unwritten remainder stays in
/// `connection.outbox` and is retried on the next writable event (or the
/// next time more data is queued).
fn flush_outbox(connection: &mut Connection) -> io::Result<()> {
    while !connection.outbox.is_empty() {
        match connection.stream.write(connection.outbox.as_slices().0) {
            // A zero-length write means the socket can't make progress at
            // all; treat it like `io::Write::write_all` does.
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => {
                connection.outbox.drain(..n);
            }
            // Would block "errors" are the OS's way of saying that the
            // connection is not actually ready to perform this I/O operation.
            // The rest of the queue stays put until the next writable event.
            Err(ref err) if would_block(err) => break,
            // Got interrupted (how rude!), we'll try again.
            Err(ref err) if interrupted(err) => continue,
            // Other errors we'll consider fatal.
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}

#[cfg(target_os = "wasi")]
fn main() {
    panic!("can't bind to an address with wasi")
}
