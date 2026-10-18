//! The per-unit hour and day a control plane charts. The times below are spelled as its object
//! keys and its readers spell them.

fn utc(secs: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default()
}
pub fn hour_key(secs: i64) -> String {
    utc(secs).format("%Y%m%d%H").to_string()
}
pub fn mmss(secs: i64) -> String {
    utc(secs).format("%M%S").to_string()
}
pub fn micros_rfc3339(micros: i64) -> String {
    let at = chrono::DateTime::from_timestamp_micros(micros).unwrap_or_default();
    at.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}
