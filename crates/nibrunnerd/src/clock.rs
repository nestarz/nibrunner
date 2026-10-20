use protocol::Timestamp;

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub fn now_us() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

pub fn now_timestamp() -> Timestamp {
    Timestamp::from_epoch_ms(now_ms())
}

#[cfg(test)]
mod tests {
    use super::*;

    const THE_START_OF_2024: i64 = 1_704_067_200_000;

    #[test]
    fn the_host_reads_a_wall_clock_rather_than_a_counter_that_starts_at_nothing() {
        let taken = now_ms();
        assert!(taken > THE_START_OF_2024, "{taken}");
        assert!(now_ms() >= taken);
    }

    #[test]
    fn a_stamped_instant_reads_back_as_the_millisecond_it_was_taken_at() {
        let taken = now_ms();
        let stamped = now_timestamp();
        assert!(
            (stamped.epoch_ms() - taken).abs() < 1_000,
            "{stamped} is not the instant {taken}"
        );
        assert_eq!(Timestamp::from_epoch_ms(taken).epoch_ms(), taken);
    }

    #[test]
    fn every_stamp_this_host_writes_is_utc_to_the_millisecond() {
        let rendered = now_timestamp().to_string();
        assert!(rendered.ends_with('Z'), "{rendered}");
        assert_eq!(rendered.len(), "2026-08-03T10:00:00.000Z".len(), "{rendered}");
    }
}
