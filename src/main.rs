use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::ops::{AddAssign, SubAssign};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::{thread, time};

// Use a fast set (non crypto and using AES)
// https://github.com/tkaitchuck/ahash#readme
use ahash::AHashSet;
// Context stack traces.
use anyhow::{Context, Result};
// Some logging tools, to avoid println!()
use log::{debug, error, info};
// Handy helper for errors.
use thiserror::Error;

const PORT: u32 = 4000;
const MAX_CLIENTS: usize = 5;
const REPORT_PERIOD_SECONDS: u64 = 10;
const FILENAME: &str = "numbers.log";
const READ_BUFFER_SIZE: usize = 1024 * 1024; // 1MB

type Number = u64;

/// A simple thread-safe counter abstraction using
/// a read/write lock.
/// Using `AtomicUsize` would also be acceptable, but
/// a `RwLock` on top of a generic feels more natural.
struct Counter<T>(RwLock<T>);

impl<T> Counter<T>
where
    T: Copy + From<u8> + AddAssign + SubAssign,
{
    fn new() -> Self {
        Counter(RwLock::new(0.into()))
    }

    fn dec(&self) {
        *self.0.write().unwrap() -= 1.into()
    }

    fn inc(&self) {
        *self.0.write().unwrap() += 1.into()
    }

    fn value(&self) -> T {
        *self.0.read().unwrap()
    }
}

/// A global thread-safe mutable to shutdown everything.
/// This is probably one of the rarest case where a global is
/// acceptable.
/// We use a relaxed atomic since this is very cheap and consistency
/// is not crucial to initiate shutdown accross threads.
static SHUTDOWN_FLAG: AtomicBool = AtomicBool::new(false);
fn is_shutdown() -> bool {
    SHUTDOWN_FLAG.load(Ordering::Relaxed)
}

#[derive(Debug, Error)]
enum ClientError {
    #[error("client sent bad data ({0})")]
    BadInput(String),
    #[error("client closed connection")]
    Disconnected,
    #[error("client read error")]
    ReadError(#[from] std::io::Error),
}

fn handle_client(
    stream: &TcpStream,
    numbers_sender: &flume::Sender<Number>,
) -> Result<(), ClientError> {
    // Use a buffered reader to get lines easily.
    let mut reader = BufReader::with_capacity(READ_BUFFER_SIZE, stream);
    let mut line = String::new();
    while !is_shutdown() {
        // Warning ⚠️: This will block on idle/slow clients.
        let size = reader.read_line(&mut line)?;
        if size == 0 {
            return Err(ClientError::Disconnected);
        } else {
            // 🐢 Simple number parsing.
            let value = line.trim_end();
            match value.parse::<Number>() {
                Ok(number) => {
                    // It's ok to panic here if all listeners are dropped.
                    // 🐢 Channel performance matters.
                    numbers_sender.send(number).unwrap();
                    line.clear();
                    // loop!
                }
                Err(_) => {
                    // Not a number.
                    if value == "terminate" {
                        // Flag state to `shutdown` and exit.
                        SHUTDOWN_FLAG.store(true, Ordering::Relaxed);
                        return Ok(());
                    }
                    return Err(ClientError::BadInput(value.into()));
                }
            }
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    env_logger::init();

    let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT)).unwrap();
    listener
        .set_nonblocking(true)
        .context("Failed to set non-blocking TcpListener")?;

    // Some counters for reporting metrics.
    let total_counter = Arc::new(Counter::new());
    let uniques_counter = Arc::new(Counter::new());
    let peers_counter = Arc::new(Counter::<usize>::new());

    // These producers-consumer channels will be used to communicate values
    // between threads.
    // `flume` is a drop-in remplacement for `std::sync::mpsc`, but faster.
    // This one will be used to pass clients values to the dedup thread.
    let (numbers_sender, numbers_receiver) = flume::unbounded();
    // This one will be used to pass uniques values to the file writer.
    let (uniques_sender, uniques_receiver) = flume::unbounded();

    // 🧵 Dedup thread!
    // Pass references of counters to the thread using clones (thanks Arc).
    let dedup_total_counter = total_counter.clone();
    let dedup_uniques_counter = uniques_counter.clone();
    let dedup_thread = thread::spawn(move || {
        // Use a set to dedup values.
        let mut uniques = AHashSet::new();
        // Block until a number is available to read in the channel.
        // Exit when `numbers_senders` are dropped.
        while let Ok(number) = numbers_receiver.recv() {
            dedup_total_counter.inc();
            // 🐢 Check unicity.
            if uniques.insert(number) {
                dedup_uniques_counter.inc();
                // Will if consumer is dropped first, panic anyway.
                uniques_sender.send(number).unwrap();
            }
        }
    });

    // 🧵 File write thread!
    let mut output =
        File::create(FILENAME).with_context(|| format!("Failed to create file {}", FILENAME))?;
    let filewrite_thread = thread::spawn(move || {
        while let Ok(number) = uniques_receiver.recv() {
            output
                .write_all(format!("{}\n", number).as_bytes())
                .with_context(|| format!("Cannot write to file {}", FILENAME))
                // Panic if cannot write to file.
                .unwrap();
        }
    });

    // 🧵 Reporter thread!
    // This thread will read the counters and report regularly.
    thread::spawn(move || {
        let mut before_uniques = 0;
        let mut before_total = 0;
        loop {
            thread::sleep(time::Duration::from_secs(REPORT_PERIOD_SECONDS));
            // How many since last report?
            let uniques = uniques_counter.value();
            let total = total_counter.value();
            let new_uniques = uniques - before_uniques;
            println!(
                "Received {} unique numbers, {} duplicates. Unique total: {}",
                new_uniques,
                total - before_total - new_uniques,
                total
            );
            // Store current situation for next iteration.
            before_uniques = uniques;
            before_total = total;
        }
    });

    // Check for incoming connections (non-blocking).
    let mut threads = vec![];
    for _stream in listener.incoming() {
        match _stream {
            Ok(stream) => {
                // Client connected.
                let remote = stream.peer_addr().unwrap();
                // Limit number of opened connections.
                let peers = peers_counter.value();
                if peers == MAX_CLIENTS {
                    debug!("Refuse {}, too many clients", remote);
                    match stream.shutdown(Shutdown::Read) {
                        Ok(_) => continue,
                        Err(e) => {
                            // Something is wrong with sockets, exit gracefully.
                            error!("Socket error: {}", e);
                            break;
                        }
                    }
                }
                // 🧵 Client(s) thread!
                // Spawn a thread per client, that will «produce» numbers.
                // The thread will end when the `shutdown` flag is set or
                // when an error occurs.
                debug!("Connection {}/{}: {}", peers + 1, MAX_CLIENTS, remote);
                peers_counter.inc();
                let numbers_sender_n = numbers_sender.clone();
                let peers_counter_n = peers_counter.clone();
                let th = thread::spawn(move || {
                    if let Err(e) = handle_client(&stream, &numbers_sender_n) {
                        debug!("Error from {}: {}", remote, e);
                    }
                    peers_counter_n.dec();
                    if let Err(e) = stream.shutdown(Shutdown::Read) {
                        // Failed to close socket. Log error, and exit thread.
                        error!("Socket error from {}: {}", remote, e);
                    }
                });
                threads.push(th);
            }
            // No connection available yet.
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // Exit if the `shutdown` flag is set
                if is_shutdown() {
                    info!("Graceful shutdown!");
                    break;
                }
                // or wait a little bit before checking again.
                thread::sleep(time::Duration::from_millis(100));
            }
            Err(e) => {
                // Something is wrong with accepting connections, exit gracefully.
                error!("IO Error: {}", e);
                break;
            }
        }
    }

    // Wait for client threads to catch the shutdown flag,
    // and eventually drop all the clones of `numbers_sender`.
    // Note: we are stuck if there are idle clients.
    debug!("Wait for clients to finish their line...");
    for th in threads {
        th.join().unwrap();
    }

    // Drop the last copy of producer for the numbers channel,
    // so that consumers can stop waiting for numbers.
    // Note: the `uniques_sender` will be dropped along
    // with the dedup thread (we didn't clone it).
    drop(numbers_sender);

    // Wait for the threads to consume the content left in the
    // channels.
    debug!("Finish writing to file...");
    dedup_thread.join().unwrap();
    filewrite_thread.join().unwrap();

    info!("Done.");
    Ok(())
}
