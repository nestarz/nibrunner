use std::io::Write;
use std::os::fd::OwnedFd;
use std::time::{Duration, Instant};

use guest_contract::channels::{Channel, ChannelTransport};
use guest_contract::logs::{encode_frame, encode_gap, encode_restart, kind_of, FRAME_HEADER_BYTES};
use protocol::{TenantLogStream, TenantRestart};

/// What the first failed dial waits before the next, and what each failure after it doubles
/// that to at most. A host daemon restarts in under half a second; one that is not listening at
/// all is soon asked no more often than it was.
const RETRY_FLOOR: Duration = Duration::from_millis(200);
const RETRY_CEILING: Duration = Duration::from_secs(5);

type Dial = Box<dyn FnMut() -> Option<OwnedFd>>;

pub(crate) struct Forwarder {
    dial: Dial,
    connection: Option<std::fs::File>,
    retry_after: Instant,
    /// What the next failed dial puts off the one after it by.
    backoff: Duration,
    dropped_bytes: u64,
}

impl Forwarder {
    pub(crate) fn new(transport: ChannelTransport) -> Self {
        Self::dialing(Box::new(move || {
            super::transport::dial(transport, Channel::Logs).ok()
        }))
    }

    pub(crate) fn dialing(dial: Dial) -> Self {
        Self {
            dial,
            connection: None,
            retry_after: Instant::now(),
            backoff: RETRY_FLOOR,
            dropped_bytes: 0,
        }
    }

    /// How long before a pending gap may be dialed out to the host; `None` with none pending,
    /// or with a connection already held to carry it.
    pub(crate) fn reconnect_due_in(&self) -> Option<Duration> {
        if self.connection.is_some() || self.dropped_bytes == 0 {
            return None;
        }
        Some(self.retry_after.saturating_duration_since(Instant::now()))
    }

    /// Dials, and declares the gap, once `reconnect_due_in` has run out: the host hears what it
    /// missed as soon as it is back and the retry allows, not when the tenant next has
    /// something to say.
    pub(crate) fn reconnect_if_due(&mut self) {
        if self.reconnect_due_in() == Some(Duration::ZERO) {
            self.redial();
        }
    }

    pub(crate) fn write(&mut self, stream: TenantLogStream, bytes: &[u8]) {
        self.send(encode_frame(kind_of(stream), bytes));
    }

    /// Sent the way the tenant's output is, resend and all: a restart the host was not there to
    /// hear of is so many bytes in the next gap, and one fewer on its count.
    pub(crate) fn restarted(&mut self, restart: &TenantRestart) {
        self.send(encode_restart(restart));
    }

    fn send(&mut self, frame: Vec<u8>) {
        let delivered = if self.connection.is_some() {
            // A connection that fails is as likely one a snapshot captured, or one a host daemon
            // since restarted was holding, as a host gone away: the write is the first news of
            // it, so a fresh connection carries the same frame before anything is given up.
            self.deliver(&frame) || (self.redial() && self.deliver(&frame))
        } else {
            Instant::now() >= self.retry_after && self.redial() && self.deliver(&frame)
        };
        if !delivered {
            self.dropped_bytes += (frame.len() - FRAME_HEADER_BYTES) as u64;
        }
    }

    /// True when the host has the frame; false with the connection gone when it has not.
    fn deliver(&mut self, frame: &[u8]) -> bool {
        let Some(connection) = &mut self.connection else {
            return false;
        };
        if connection.write_all(frame).is_ok() {
            return true;
        }
        self.connection = None;
        false
    }

    /// A fresh connection, told first what it missed. Whether or not it comes, the next
    /// unprompted attempt at one waits: the floor after a success, the backoff after a failure,
    /// which the failure doubles for the one after that.
    fn redial(&mut self) -> bool {
        self.connection = (self.dial)().map(std::fs::File::from);
        if self.connection.is_some() && self.declare_gap() {
            self.backoff = RETRY_FLOOR;
            self.retry_after = Instant::now() + RETRY_FLOOR;
            return true;
        }
        self.retry_after = Instant::now() + self.backoff;
        self.backoff = (self.backoff * 2).min(RETRY_CEILING);
        false
    }

    fn declare_gap(&mut self) -> bool {
        if self.dropped_bytes == 0 {
            return true;
        }
        if !self.deliver(&encode_gap(self.dropped_bytes)) {
            return false;
        }
        self.dropped_bytes = 0;
        true
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::io::Read;
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::Path;
    use std::rc::Rc;

    use guest_contract::logs::{decode_frames, GuestLogFrame};
    use protocol::{StateMessage, TenantExit};

    use super::*;

    /// A host listening at `path`, counting how many times the forwarder asked for it.
    struct Host {
        listener: UnixListener,
        dials: Rc<Cell<u32>>,
    }

    impl Host {
        fn listening_at(path: &Path) -> Self {
            let listener = UnixListener::bind(path).unwrap();
            listener.set_nonblocking(true).unwrap();
            Self {
                listener,
                dials: Rc::new(Cell::new(0)),
            }
        }

        fn forwarder(&self, path: &Path) -> Forwarder {
            forwarder_dialing(path, &self.dials)
        }

        fn accept(&self) -> UnixStream {
            let (connection, _) = self.listener.accept().unwrap();
            connection.set_nonblocking(true).unwrap();
            connection
        }

        fn nothing_pending(&self) {
            assert!(self.listener.accept().is_err(), "a connection nobody dialed");
        }

        /// Gone, until one is `listening_at` the path again.
        fn away(self, path: &Path) {
            drop(self);
            std::fs::remove_file(path).unwrap();
        }
    }

    /// A forwarder dialing `path`, counting every dial on `dials`, which outlives any one host.
    fn forwarder_dialing(path: &Path, dials: &Rc<Cell<u32>>) -> Forwarder {
        let path = path.to_path_buf();
        let dials = Rc::clone(dials);
        Forwarder::dialing(Box::new(move || {
            dials.set(dials.get() + 1);
            UnixStream::connect(&path).ok().map(OwnedFd::from)
        }))
    }

    fn arrived(connection: &mut UnixStream) -> Vec<GuestLogFrame> {
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match connection.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => bytes.extend_from_slice(&chunk[..read]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("{error}"),
            }
        }
        let (frames, rest) = decode_frames(&[], &bytes).unwrap();
        assert!(rest.is_empty(), "a frame arrived in part");
        frames
    }

    /// The host's end of a connection, gone. Shut down and not only dropped: the supervisor's
    /// tests fork this process, and a child of theirs may hold a copy of the descriptor that
    /// would keep a dropped socket open.
    fn gone(connection: UnixStream) {
        connection.shutdown(std::net::Shutdown::Both).unwrap();
    }

    fn stdout(text: &str) -> GuestLogFrame {
        GuestLogFrame::Data {
            stream: TenantLogStream::Stdout,
            bytes: text.as_bytes().to_vec(),
        }
    }

    #[test]
    fn a_write_that_fails_once_is_resent_on_a_fresh_connection_and_nothing_is_declared_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logs.sock");
        let host = Host::listening_at(&path);
        let mut forwarder = host.forwarder(&path);

        forwarder.write(TenantLogStream::Stdout, b"before\n");
        let mut first = host.accept();
        assert_eq!(arrived(&mut first), vec![stdout("before\n")]);

        // The host daemon restarts: what it held is gone, and it listens afresh.
        gone(first);
        forwarder.write(TenantLogStream::Stdout, b"after\n");

        let mut second = host.accept();
        assert_eq!(arrived(&mut second), vec![stdout("after\n")]);
        assert_eq!(host.dials.get(), 2);
        assert_eq!(forwarder.dropped_bytes, 0);
    }

    fn restart() -> TenantRestart {
        TenantRestart {
            attempt: 1,
            budget: 5,
            exit: TenantExit::Signal(9),
            reason: StateMessage::new("the tenant exited (137); restart 1 of 5 in 500ms"),
            backoff_ms: 500,
        }
    }

    #[test]
    fn a_restart_takes_its_place_after_the_output_and_is_resent_like_it_on_a_fresh_connection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logs.sock");
        let host = Host::listening_at(&path);
        let mut forwarder = host.forwarder(&path);

        forwarder.write(TenantLogStream::Stderr, b"out of memory\n");
        forwarder.restarted(&restart());
        let mut first = host.accept();
        assert_eq!(
            arrived(&mut first),
            vec![
                GuestLogFrame::Data {
                    stream: TenantLogStream::Stderr,
                    bytes: b"out of memory\n".to_vec(),
                },
                GuestLogFrame::Restart(restart()),
            ]
        );

        gone(first);
        forwarder.restarted(&restart());
        let mut second = host.accept();
        assert_eq!(arrived(&mut second), vec![GuestLogFrame::Restart(restart())]);
        assert_eq!(forwarder.dropped_bytes, 0);
    }

    #[test]
    fn a_restart_nobody_heard_is_so_many_bytes_of_the_gap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logs.sock");
        let mut forwarder = Forwarder::dialing(Box::new(move || {
            UnixStream::connect(&path).ok().map(OwnedFd::from)
        }));
        forwarder.restarted(&restart());
        let payload_bytes = encode_restart(&restart()).len() - FRAME_HEADER_BYTES;
        assert_eq!(forwarder.dropped_bytes, payload_bytes as u64);
    }

    #[test]
    fn a_host_that_stays_down_accumulates_the_gap_and_hears_it_on_reconnect() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logs.sock");
        let host = Host::listening_at(&path);
        let mut forwarder = host.forwarder(&path);
        forwarder.write(TenantLogStream::Stdout, b"before\n");
        let mut first = host.accept();
        assert_eq!(arrived(&mut first), vec![stdout("before\n")]);

        // Gone, and not back: the resend finds nobody listening.
        gone(first);
        host.away(&path);
        forwarder.write(TenantLogStream::Stdout, b"one\n");
        assert_eq!(forwarder.dropped_bytes, 4);
        forwarder.write(TenantLogStream::Stdout, b"two\n");
        assert_eq!(forwarder.dropped_bytes, 8);

        // Back, but the forwarder is not due to ask yet.
        let host = Host::listening_at(&path);
        forwarder.write(TenantLogStream::Stderr, b"three\n");
        assert_eq!(forwarder.dropped_bytes, 14);
        host.nothing_pending();

        forwarder.retry_after = Instant::now();
        forwarder.write(TenantLogStream::Stdout, b"four\n");
        let mut reconnected = host.accept();
        assert_eq!(
            arrived(&mut reconnected),
            vec![GuestLogFrame::Gap { dropped_bytes: 14 }, stdout("four\n")]
        );
        assert_eq!(forwarder.dropped_bytes, 0);
    }

    #[test]
    fn a_host_that_was_never_there_is_asked_once_and_then_left_alone_for_a_while() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logs.sock");
        let dials = Rc::new(Cell::new(0));
        let mut forwarder = forwarder_dialing(&path, &dials);
        for _ in 0..10 {
            forwarder.write(TenantLogStream::Stdout, b"line\n");
        }
        assert_eq!(dials.get(), 1);
        assert_eq!(forwarder.dropped_bytes, 50);
    }

    #[test]
    fn a_host_back_within_the_floor_of_one_failed_dial_hears_the_next_line_then_not_seconds_later() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logs.sock");
        let dials = Rc::new(Cell::new(0));
        let host = Host::listening_at(&path);
        let mut forwarder = forwarder_dialing(&path, &dials);
        forwarder.write(TenantLogStream::Stdout, b"before\n");
        let mut first = host.accept();
        assert_eq!(arrived(&mut first), vec![stdout("before\n")]);

        // The host daemon restarts: the resend finds it not listening yet.
        gone(first);
        host.away(&path);
        let failed_at = Instant::now();
        forwarder.write(TenantLogStream::Stdout, b"one\n");
        assert_eq!(dials.get(), 2);
        assert_eq!(forwarder.dropped_bytes, 4);
        assert!(forwarder.retry_after <= Instant::now() + RETRY_FLOOR);

        // Back well inside the floor, as a restarted daemon is.
        let host = Host::listening_at(&path);
        std::thread::sleep(forwarder.retry_after.saturating_duration_since(Instant::now()));
        forwarder.write(TenantLogStream::Stdout, b"two\n");
        let mut second = host.accept();
        assert_eq!(
            arrived(&mut second),
            vec![GuestLogFrame::Gap { dropped_bytes: 4 }, stdout("two\n")]
        );
        assert_eq!(dials.get(), 3);
        assert_eq!(forwarder.dropped_bytes, 0);
        let asked_again = failed_at.elapsed();
        assert!(
            asked_again < Duration::from_secs(1),
            "asked again after {asked_again:?}"
        );
    }

    #[test]
    fn every_refused_dial_doubles_the_wait_for_the_next_up_to_the_ceiling_and_one_taken_resets_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logs.sock");
        let dials = Rc::new(Cell::new(0));
        let mut forwarder = forwarder_dialing(&path, &dials);
        let mut wait = RETRY_FLOOR;
        for round in 1..=7 {
            forwarder.retry_after = Instant::now();
            let asked_at = Instant::now();
            forwarder.write(TenantLogStream::Stdout, b"line\n");
            assert_eq!(dials.get(), round);
            assert!(
                forwarder.retry_after >= asked_at + wait,
                "round {round} waits less than {wait:?}"
            );
            assert!(
                forwarder.retry_after <= Instant::now() + wait,
                "round {round} waits more than {wait:?}"
            );
            wait = (wait * 2).min(RETRY_CEILING);
        }
        assert_eq!(wait, RETRY_CEILING, "seven rounds reach the ceiling");
        assert_eq!(forwarder.dropped_bytes, 35);

        // A host that takes a dial puts the wait back at the floor for the next refusal.
        let host = Host::listening_at(&path);
        forwarder.retry_after = Instant::now();
        forwarder.write(TenantLogStream::Stdout, b"back\n");
        let mut connection = host.accept();
        assert_eq!(
            arrived(&mut connection),
            vec![GuestLogFrame::Gap { dropped_bytes: 35 }, stdout("back\n")]
        );
        gone(connection);
        host.away(&path);
        forwarder.write(TenantLogStream::Stdout, b"line\n");
        assert_eq!(dials.get(), 9);
        assert!(forwarder.retry_after <= Instant::now() + RETRY_FLOOR);
    }

    #[test]
    fn a_forwarder_with_no_gap_to_declare_has_no_dial_due() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logs.sock");
        let dials = Rc::new(Cell::new(0));
        let mut forwarder = forwarder_dialing(&path, &dials);
        assert_eq!(forwarder.reconnect_due_in(), None);
        forwarder.reconnect_if_due();
        assert_eq!(dials.get(), 0);
    }

    #[test]
    fn a_pending_gap_is_dialed_out_when_due_without_the_tenant_saying_anything_more() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logs.sock");
        let dials = Rc::new(Cell::new(0));
        let host = Host::listening_at(&path);
        let mut forwarder = forwarder_dialing(&path, &dials);
        forwarder.write(TenantLogStream::Stdout, b"before\n");
        let mut first = host.accept();
        assert_eq!(arrived(&mut first), vec![stdout("before\n")]);
        gone(first);
        host.away(&path);
        forwarder.write(TenantLogStream::Stdout, b"one\n");
        assert_eq!(dials.get(), 2);
        assert_eq!(forwarder.dropped_bytes, 4);

        // Still down: asked when due, and the refusal puts the next ask further off.
        forwarder.retry_after = Instant::now();
        forwarder.reconnect_if_due();
        assert_eq!(dials.get(), 3);
        let due_in = forwarder.reconnect_due_in().unwrap();
        assert!(due_in > RETRY_FLOOR && due_in <= 2 * RETRY_FLOOR, "{due_in:?}");

        // Back, but not due: not asked.
        let host = Host::listening_at(&path);
        forwarder.reconnect_if_due();
        host.nothing_pending();
        assert_eq!(dials.get(), 3);

        // Due: the gap alone, on a fresh connection, with the tenant silent.
        forwarder.retry_after = Instant::now();
        assert_eq!(forwarder.reconnect_due_in(), Some(Duration::ZERO));
        forwarder.reconnect_if_due();
        let mut reconnected = host.accept();
        assert_eq!(
            arrived(&mut reconnected),
            vec![GuestLogFrame::Gap { dropped_bytes: 4 }]
        );
        assert_eq!(dials.get(), 4);
        assert_eq!(forwarder.dropped_bytes, 0);

        // Connected, there is nothing to dial for, and the next line takes that connection.
        assert_eq!(forwarder.reconnect_due_in(), None);
        forwarder.reconnect_if_due();
        forwarder.write(TenantLogStream::Stdout, b"two\n");
        assert_eq!(arrived(&mut reconnected), vec![stdout("two\n")]);
        assert_eq!(dials.get(), 4);
    }
}
