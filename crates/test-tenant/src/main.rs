//! What the guest tests deploy. Every route exists so that one invariant can be asked of a
//! running guest from outside it: whether it kept what it had in memory, whether what it wrote
//! is still there, whether the kernel let it reach somewhere it should not have.
//!
//! No dependencies and no runtime: a thread per connection, and the whole thing is one static
//! musl binary small enough to hand to the artifact store as a layer.

// A tenant the tests boot, and every way it can die is one of them asking it to.
#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Lives as long as the process, so a wake that restored the microVM answers with the count it
/// was holding and a wake that booted a fresh one starts over.
static REMEMBERED: AtomicU64 = AtomicU64::new(0);

const REACH_TIMEOUT: Duration = Duration::from_secs(2);

struct Settings {
    data_dir: PathBuf,
    listen_after: Duration,
    never_listen: bool,
    raw_tcp: Option<u16>,
    raw_udp: Option<u16>,
}

fn settings() -> Settings {
    let mut held = Settings {
        // Under the working directory, which is the one the guest gives this uid: everything
        // above it belongs to root, and a tenant cannot make a directory there.
        data_dir: PathBuf::from("/app/data"),
        listen_after: Duration::ZERO,
        never_listen: false,
        raw_tcp: None,
        raw_udp: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || args.next().unwrap_or_default();
        match flag.as_str() {
            "--data-dir" => held.data_dir = PathBuf::from(value()),
            "--listen-after-ms" => held.listen_after = Duration::from_millis(value().parse().unwrap_or(0)),
            "--never-listen" => held.never_listen = true,
            "--raw-tcp" => held.raw_tcp = value().parse().ok(),
            "--raw-udp" => held.raw_udp = value().parse().ok(),
            _ => {}
        }
    }
    held
}

fn main() {
    let settings = settings();
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|port| port.parse().ok())
        .unwrap_or(3000);

    if let Some(raw) = settings.raw_tcp {
        std::thread::spawn(move || echo_stream(raw));
    }
    if let Some(raw) = settings.raw_udp {
        std::thread::spawn(move || echo_datagram(raw));
    }

    if settings.never_listen {
        println!("this tenant was told never to listen");
        loop {
            std::thread::sleep(Duration::from_secs(3600));
        }
    }
    if !settings.listen_after.is_zero() {
        std::thread::sleep(settings.listen_after);
    }

    let listener = TcpListener::bind(("0.0.0.0", port)).expect("the port this tenant was given");
    println!("listening on {port}");
    for stream in listener.incoming().flatten() {
        let data_dir = settings.data_dir.clone();
        std::thread::spawn(move || serve(stream, &data_dir));
    }
}

/// One connection, for as long as the caller keeps it: the proxy pools these, and a tenant that
/// closed after every response would make the pooled-connection invariants untestable.
fn serve(stream: TcpStream, data_dir: &Path) {
    let mut writing = match stream.try_clone() {
        Ok(writing) => writing,
        Err(_) => return,
    };
    let mut reading = BufReader::new(stream);
    loop {
        let mut request_line = String::new();
        if reading.read_line(&mut request_line).unwrap_or(0) == 0 {
            return;
        }
        let mut length = 0usize;
        let mut closing = false;
        loop {
            let mut header = String::new();
            if reading.read_line(&mut header).unwrap_or(0) == 0 {
                return;
            }
            let header = header.trim_end().to_ascii_lowercase();
            if header.is_empty() {
                break;
            }
            if let Some(said) = header.strip_prefix("content-length:") {
                length = said.trim().parse().unwrap_or(0);
            }
            if let Some(said) = header.strip_prefix("connection:") {
                closing = said.contains("close");
            }
        }
        if length > 0 {
            let mut body = vec![0u8; length];
            if reading.read_exact(&mut body).is_err() {
                return;
            }
        }

        let target = request_line.split_whitespace().nth(1).unwrap_or("/").to_string();
        let (status, body) = answer(&target, data_dir);
        // A caller that asked to be done is told the same and let go. Everyone else keeps the
        // connection, which is what makes the proxy's pooling of it worth testing.
        let sent = write!(
            writing,
            "HTTP/1.1 {status}\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: {}\r\n\r\n{body}",
            body.len(),
            if closing { "close" } else { "keep-alive" }
        )
        .and_then(|()| writing.flush());
        if sent.is_err() {
            return;
        }
        if target_path(&target) == "/exit" {
            std::process::exit(exit_code(&target));
        }
        if closing {
            return;
        }
    }
}

fn target_path(target: &str) -> &str {
    target.split('?').next().unwrap_or(target)
}

fn exit_code(target: &str) -> i32 {
    query(target, "code")
        .and_then(|code| code.parse().ok())
        .unwrap_or(0)
}

fn query(target: &str, name: &str) -> Option<String> {
    let asked = target.split_once('?')?.1;
    asked
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| *key == name)
        .map(|(_, value)| decode(value))
}

fn decode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                match u8::from_str_radix(&value[index + 1..index + 3], 16) {
                    Ok(byte) => out.push(byte as char),
                    Err(_) => out.push('%'),
                }
                index += 3;
            }
            byte => {
                out.push(byte as char);
                index += 1;
            }
        }
    }
    out
}

fn answer(target: &str, data_dir: &Path) -> (&'static str, String) {
    match target_path(target) {
        "/" => ("200 OK", "ok".to_string()),
        "/remember" => (
            "200 OK",
            (REMEMBERED.fetch_add(1, Ordering::SeqCst) + 1).to_string(),
        ),
        "/write" => write_file(target, data_dir),
        "/read" => read_file(target, data_dir),
        "/log" => {
            let lines: u64 = query(target, "lines").and_then(|n| n.parse().ok()).unwrap_or(1);
            let mut out = std::io::stdout().lock();
            for line in 1..=lines {
                let _ = writeln!(out, "line {line}");
            }
            let _ = out.flush();
            ("200 OK", lines.to_string())
        }
        "/hang" => {
            let ms: u64 = query(target, "ms").and_then(|n| n.parse().ok()).unwrap_or(0);
            std::thread::sleep(Duration::from_millis(ms));
            ("200 OK", "waited".to_string())
        }
        "/reach" => ("200 OK", reach(&query(target, "addr").unwrap_or_default())),
        "/env" => (
            "200 OK",
            std::env::var(query(target, "name").unwrap_or_default()).unwrap_or_default(),
        ),
        "/exit" => ("200 OK", "going".to_string()),
        _ => ("404 Not Found", "no such route".to_string()),
    }
}

/// Never outside the directory it was given: a test that asked for `..` is a test with a bug,
/// and one that quietly wrote over the guest's own root would be hard to see.
fn within(data_dir: &Path, asked: &str) -> Option<PathBuf> {
    let asked = Path::new(asked.trim_start_matches('/'));
    if asked
        .components()
        .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return None;
    }
    Some(data_dir.join(asked))
}

fn write_file(target: &str, data_dir: &Path) -> (&'static str, String) {
    let Some(path) = query(target, "path").and_then(|asked| within(data_dir, &asked)) else {
        return ("400 Bad Request", "that is not a path".to_string());
    };
    let body = query(target, "body").unwrap_or_default();
    if let Some(parent) = path.parent() {
        if let Err(error) = std::fs::create_dir_all(parent) {
            return ("500 Internal Server Error", error.to_string());
        }
    }
    // Committed, not merely written: a stopped app's microVM is killed rather than shut down, so
    // what is still in the guest's page cache dies with it and would read as a volume that lost
    // it. What this answers for is what a tenant made durable.
    let written = std::fs::File::create(&path).and_then(|mut file| {
        file.write_all(body.as_bytes())?;
        file.sync_all()
    });
    match written {
        Ok(()) => ("200 OK", body.len().to_string()),
        Err(error) => ("500 Internal Server Error", error.to_string()),
    }
}

fn read_file(target: &str, data_dir: &Path) -> (&'static str, String) {
    let Some(path) = query(target, "path").and_then(|asked| within(data_dir, &asked)) else {
        return ("400 Bad Request", "that is not a path".to_string());
    };
    match std::fs::read_to_string(&path) {
        Ok(held) => ("200 OK", held),
        Err(error) => ("404 Not Found", error.to_string()),
    }
}

/// What the kernel did with one outbound connection, said plainly enough for a test to assert on
/// without knowing whether a refused route shows up as unreachable, refused or a timeout.
fn reach(address: &str) -> String {
    let Ok(mut resolved) = address.to_socket_addrs() else {
        return "unresolved".to_string();
    };
    let Some(resolved) = resolved.next() else {
        return "unresolved".to_string();
    };
    match TcpStream::connect_timeout(&resolved, REACH_TIMEOUT) {
        Ok(_) => "reached".to_string(),
        Err(error) => format!("blocked: {error}"),
    }
}

fn echo_stream(port: u16) {
    let Ok(listener) = TcpListener::bind(("0.0.0.0", port)) else {
        return;
    };
    for stream in listener.incoming().flatten() {
        std::thread::spawn(move || {
            let mut stream = stream;
            let mut buffer = [0u8; 1024];
            while let Ok(read) = stream.read(&mut buffer) {
                if read == 0 || stream.write_all(&buffer[..read]).is_err() {
                    return;
                }
            }
        });
    }
}

fn echo_datagram(port: u16) {
    let Ok(socket) = UdpSocket::bind(("0.0.0.0", port)) else {
        return;
    };
    let mut buffer = [0u8; 1024];
    while let Ok((read, from)) = socket.recv_from(&mut buffer) {
        let _ = socket.send_to(&buffer[..read], from);
    }
}
