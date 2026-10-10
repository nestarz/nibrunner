//! The tenant the KVM gate deploys: one static binary, no dependencies, built with `rustc` alone.
//!
//!   tenant           answer HTTP on :3000 — `/`, `/fill?mib=N` writes N MiB to ./fill and syncs
//!                    it, `/size` says how large ./fill is, or `absent`
//!   tenant exit N    say so on stdout and exit with N

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};

const FILL: &str = "fill";
const MIB: usize = 1024 * 1024;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let [mode, code] = args.as_slice() {
        if mode == "exit" {
            let code: i32 = code.parse().expect("an exit code");
            println!("exiting with {code}");
            std::process::exit(code);
        }
    }
    let listener = TcpListener::bind(("0.0.0.0", 3000)).expect("port 3000");
    println!("listening on 3000");
    for stream in listener.incoming().flatten() {
        std::thread::spawn(move || serve(stream));
    }
}

fn serve(stream: TcpStream) {
    let mut request_line = String::new();
    let mut reading = BufReader::new(&stream);
    if reading.read_line(&mut request_line).is_err() {
        return;
    }
    let mut header = String::new();
    while reading.read_line(&mut header).is_ok_and(|read| read > 2) {
        header.clear();
    }
    let target = request_line.split_whitespace().nth(1).unwrap_or("/");
    let (status, body) = answer(target);
    let _ = write!(
        &stream,
        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
}

fn answer(target: &str) -> (&'static str, String) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    match path {
        "/" => ("200 OK", "ok".into()),
        "/size" => (
            "200 OK",
            std::fs::metadata(FILL).map_or("absent".into(), |held| held.len().to_string()),
        ),
        "/fill" => {
            let mib = query.strip_prefix("mib=").and_then(|mib| mib.parse().ok()).unwrap_or(0);
            match fill(mib) {
                Ok(written) => ("200 OK", written.to_string()),
                Err(error) => ("500 Internal Server Error", error.to_string()),
            }
        }
        _ => ("404 Not Found", "no such route".into()),
    }
}

fn fill(mib: usize) -> std::io::Result<usize> {
    let mut file = std::fs::File::create(FILL)?;
    let chunk = vec![0xa5u8; MIB];
    for _ in 0..mib {
        file.write_all(&chunk)?;
    }
    file.sync_all()?;
    Ok(mib * MIB)
}
