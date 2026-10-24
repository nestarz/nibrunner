use protocol::{TenantLogStream, Timestamp};

use crate::ports::{TenantLogBody, TenantLogEvent};

use super::scheduler::ScheduledRun;

const MAX_LINE_BYTES: usize = 16_384;

pub struct CronRunOutput {
    run: ScheduledRun,
    source_id: String,
    job_id: String,
    run_id: String,
    sequence: u64,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl CronRunOutput {
    pub fn new(run: ScheduledRun) -> Self {
        let source_id = uuid::Uuid::new_v4().to_string();
        let job_id = run.job.id.clone();
        let run_id = uuid::Uuid::new_v4().to_string();
        Self {
            run,
            source_id,
            job_id,
            run_id,
            sequence: 0,
            stdout: Vec::new(),
            stderr: Vec::new(),
        }
    }

    fn event(&mut self, stream: TenantLogStream, text: String) -> TenantLogEvent {
        let event = TenantLogEvent {
            app_id: self.run.app_id.clone(),
            deployment_id: self.run.deployment_id.clone(),
            source_id: self.source_id.clone(),
            sequence: self.sequence,
            observed_at: Timestamp::from_epoch_ms(crate::clock::now_ms()),
            body: TenantLogBody::CronData {
                stream,
                text,
                job_id: self.job_id.clone(),
                run_id: self.run_id.clone(),
            },
        };
        self.sequence += 1;
        event
    }

    pub fn receive(&mut self, stream: TenantLogStream, bytes: &[u8]) -> Vec<TenantLogEvent> {
        let held = match stream {
            TenantLogStream::Stdout => &mut self.stdout,
            TenantLogStream::Stderr => &mut self.stderr,
        };
        let mut lines = Vec::new();
        for byte in bytes {
            if *byte == b'\n' {
                lines.push(String::from_utf8_lossy(held).into_owned());
                held.clear();
            } else {
                held.push(*byte);
                if held.len() >= MAX_LINE_BYTES {
                    let cut = match std::str::from_utf8(held) {
                        Err(error) if error.error_len().is_none() => error.valid_up_to(),
                        _ => held.len(),
                    };
                    lines.push(String::from_utf8_lossy(&held[..cut]).into_owned());
                    held.drain(..cut);
                }
            }
        }
        lines.into_iter().map(|text| self.event(stream, text)).collect()
    }

    pub fn close(&mut self) -> Vec<TenantLogEvent> {
        let mut events = Vec::new();
        for stream in [TenantLogStream::Stdout, TenantLogStream::Stderr] {
            let bytes = match stream {
                TenantLogStream::Stdout => std::mem::take(&mut self.stdout),
                TenantLogStream::Stderr => std::mem::take(&mut self.stderr),
            };
            if !bytes.is_empty() {
                events.push(self.event(stream, String::from_utf8_lossy(&bytes).into_owned()));
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::cron_run;

    fn output() -> CronRunOutput {
        CronRunOutput::new(cron_run("echo secret"))
    }

    fn text(event: &TenantLogEvent) -> &str {
        let TenantLogBody::CronData { text, .. } = &event.body else {
            panic!("output is a data event");
        };
        text
    }

    #[test]
    fn split_characters_and_lines_keep_their_run_and_order() {
        let mut first = output();
        assert!(first.receive(TenantLogStream::Stdout, &[0xc3]).is_empty());
        let events = first.receive(TenantLogStream::Stdout, &[0xa9, b'\n', b'x']);
        assert_eq!(text(&events[0]), "é");
        assert_eq!(events[0].sequence, 0);
        let tail = first.close();
        assert_eq!(text(&tail[0]), "x");
        assert_eq!(tail[0].sequence, 1);
        assert_eq!(tail[0].source_id, events[0].source_id);
        assert!(!events[0].source_id.contains("secret"));
    }

    #[test]
    fn each_execution_has_its_own_run_and_source_but_keeps_the_registered_job_id() {
        let first = output().receive(TenantLogStream::Stdout, b"first\n").remove(0);
        let second = output().receive(TenantLogStream::Stderr, b"second\n").remove(0);
        let TenantLogBody::CronData {
            job_id: first_job,
            run_id: first_run,
            ..
        } = first.body
        else {
            panic!("cron output carries its identity")
        };
        let TenantLogBody::CronData {
            job_id: second_job,
            run_id: second_run,
            ..
        } = second.body
        else {
            panic!("cron output carries its identity")
        };
        assert_eq!(first_job, second_job);
        assert_ne!(first_run, second_run);
        assert_ne!(first.source_id, second.source_id);
        assert_ne!(first.source_id, first_run);
    }

    #[test]
    fn an_unending_line_never_grows_past_the_output_bound() {
        let mut output = output();
        for _ in 0..10 {
            let events = output.receive(TenantLogStream::Stderr, &vec![b'x'; MAX_LINE_BYTES + 3]);
            assert_eq!(events.len(), 1);
            assert_eq!(text(&events[0]).len(), MAX_LINE_BYTES);
            assert!(output.stderr.len() < MAX_LINE_BYTES);
        }
    }
}
