// You can run this example from the root of the mio repo:
// cargo run --example tcp_server --features="os-poll net"
use mio::event::Event;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Registry, Token};
use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::str::from_utf8;
use std::time::Duration;

// Setup some tokens to allow us to identify which event is for which socket.
const SERVER: Token = Token(0);

// Some data we'll send over the connection.
const DATA: &[u8] = b"Hello world!\n";

/// Maximum number of reads to handle for a single connection before giving
/// other connections a chance to make progress.
const MAX_READS_PER_EVENT: usize = 32;

struct ConnectionState {
    done: bool,
    read_pending: bool,
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

    // Map of `Token` -> `TcpStream`.
    let mut connections = HashMap::new();
    // Connections that still have data to read after hitting the per-event
    // read limit.
    let mut pending_reads = VecDeque::new();
    // Unique token for each incoming connection.
    let mut unique_token = Token(SERVER.0 + 1);

    println!("You can connect to the server using `nc`:");
    println!(" $ nc 127.0.0.1 9000");
    println!("You'll see our welcome message and anything you type will be printed here.");

    loop {
        // Don't block while there is queued work from a previous event.
        let timeout = if pending_reads.is_empty() {
            None
        } else {
            Some(Duration::ZERO)
        };
        if let Err(err) = poll.poll(&mut events, timeout) {
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
                    let (mut connection, address) = match server.accept() {
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
                    poll.registry().register(
                        &mut connection,
                        token,
                        Interest::READABLE.add(Interest::WRITABLE),
                    )?;

                    connections.insert(token, connection);
                },
                token => {
                    // Maybe received an event for a TCP connection.
                    let state = if let Some(connection) = connections.get_mut(&token) {
                        handle_connection_event(poll.registry(), connection, event)?
                    } else {
                        // Sporadic events happen, we can safely ignore them.
                        ConnectionState {
                            done: false,
                            read_pending: false,
                        }
                    };
                    if state.done {
                        if let Some(mut connection) = connections.remove(&token) {
                            poll.registry().deregister(&mut connection)?;
                        }
                        pending_reads.retain(|pending| *pending != token);
                    } else if state.read_pending {
                        if !pending_reads.contains(&token) {
                            pending_reads.push_back(token);
                        }
                    } else {
                        pending_reads.retain(|pending| *pending != token);
                    }
                }
            }
        }

        // Resume one connection at a time so a busy socket can't monopolise
        // the event loop and starve the others.
        if let Some(token) = pending_reads.pop_front() {
            if let Some(connection) = connections.get_mut(&token) {
                let state = read_connection(connection)?;
                if state.done {
                    if let Some(mut connection) = connections.remove(&token) {
                        poll.registry().deregister(&mut connection)?;
                    }
                } else if state.read_pending {
                    pending_reads.push_back(token);
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

fn handle_connection_event(
    registry: &Registry,
    connection: &mut TcpStream,
    event: &Event,
) -> io::Result<ConnectionState> {
    if event.is_writable() {
        // We can (maybe) write to the connection.
        match connection.write(DATA) {
            // We want to write the entire `DATA` buffer in a single go. If we
            // write less we'll return a short write error (same as
            // `io::Write::write_all` does).
            Ok(n) if n < DATA.len() => return Err(io::ErrorKind::WriteZero.into()),
            Ok(_) => {
                // After we've written something we'll reregister the connection
                // to only respond to readable events.
                registry.reregister(connection, event.token(), Interest::READABLE)?
            }
            // Would block "errors" are the OS's way of saying that the
            // connection is not actually ready to perform this I/O operation.
            Err(ref err) if would_block(err) => {}
            // Got interrupted (how rude!), we'll try again.
            Err(ref err) if interrupted(err) => {
                return handle_connection_event(registry, connection, event)
            }
            // Other errors we'll consider fatal.
            Err(err) => return Err(err),
        }
    }

    if event.is_readable() {
        return read_connection(connection);
    }

    Ok(ConnectionState {
        done: false,
        read_pending: false,
    })
}

/// Read until the socket would block, or until this connection has used its
/// share of the current event loop iteration.
fn read_connection(connection: &mut TcpStream) -> io::Result<ConnectionState> {
    let mut connection_closed = false;
    let mut received_data = vec![0; 4096];
    let mut bytes_read = 0;
    let mut reads = 0;

    while reads < MAX_READS_PER_EVENT {
        match connection.read(&mut received_data[bytes_read..]) {
            Ok(0) => {
                // Reading 0 bytes means the other side has closed the
                // connection or is done writing, then so are we.
                connection_closed = true;
                break;
            }
            Ok(n) => {
                bytes_read += n;
                reads += 1;
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
    }

    if connection_closed {
        println!("Connection closed");
    }

    Ok(ConnectionState {
        done: connection_closed,
        // Hitting the limit doesn't prove that the socket is drained, so keep
        // the connection queued for another batch.
        read_pending: !connection_closed && reads == MAX_READS_PER_EVENT,
    })
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
