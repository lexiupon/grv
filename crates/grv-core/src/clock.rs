//! Shared wall-clock leases and monotonic excessive-expiry observations.
use crate::store::{Result, public_error};
use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use grv_storage::{Validator, model::StoreParameters};
use grv_types::{ErrorCode, RunId, Timestamp};
use std::time::{Duration, Instant};
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
    fn elapsed(&self) -> Duration;
}
pub struct SystemClock(Instant);
impl Default for SystemClock {
    fn default() -> Self {
        Self(Instant::now())
    }
}
impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        timestamp(Utc::now())
    }
    fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
}
pub fn timestamp(value: DateTime<Utc>) -> Timestamp {
    Timestamp::new(value.to_rfc3339_opts(SecondsFormat::Nanos, true)).expect("UTC clock timestamp")
}
pub fn parse(value: &Timestamp) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value.as_str())
        .expect("validated timestamp")
        .with_timezone(&Utc)
}
pub fn expires(now: &Timestamp, ttl: u64, parameters: &StoreParameters) -> Result<Timestamp> {
    if ttl > parameters.max_lease_ttl_seconds.get()
        || ttl <= parameters.max_clock_skew_seconds.get()
    {
        return Err(public_error(
            ErrorCode::InvalidArgument,
            "lease TTL must exceed clock skew and stay within store maximum",
        ));
    }
    let seconds = i64::try_from(ttl)
        .ok()
        .and_then(chrono::Duration::try_seconds)
        .ok_or_else(|| public_error(ErrorCode::InvalidArgument, "lease TTL exceeds clock range"))?;
    parse(now)
        .checked_add_signed(seconds)
        .filter(|value| (1..=9999).contains(&value.year()))
        .map(timestamp)
        .ok_or_else(|| {
            public_error(
                ErrorCode::InvalidArgument,
                "lease expiry exceeds clock range",
            )
        })
}
#[derive(Default)]
pub struct ExpiryObservation {
    value: Option<(Validator, Duration)>,
}
impl ExpiryObservation {
    pub fn expired(
        &mut self,
        validator: &Validator,
        expiry: &Timestamp,
        clock: &dyn Clock,
        parameters: &StoreParameters,
    ) -> bool {
        if parse(&clock.now()) >= parse(expiry) {
            return true;
        }
        let seen = clock.elapsed();
        let limit = Duration::from_secs(
            parameters
                .max_lease_ttl_seconds
                .get()
                .saturating_add(parameters.max_clock_skew_seconds.get()),
        );
        match &self.value {
            Some((previous, first)) if previous == validator => {
                seen.saturating_sub(*first) >= limit
            }
            _ => {
                self.value = Some((validator.clone(), seen));
                false
            }
        }
    }
}
pub fn new_run_id(now: &Timestamp) -> Result<RunId> {
    let milliseconds = u64::try_from(parse(now).timestamp_millis())
        .ok()
        .filter(|v| *v < (1 << 48))
        .ok_or_else(|| {
            public_error(ErrorCode::InvalidArgument, "ULID clock exceeds48-bit range")
        })?;
    let random = uuid::Uuid::new_v4();
    let bytes = random.as_bytes();
    // Skip UUID's version and variant bytes: all80 random bits remain random.
    let mut entropy = [0u8; 16];
    entropy[6..12].copy_from_slice(&bytes[..6]);
    entropy[12..].copy_from_slice(&bytes[9..13]);
    let mut value = (u128::from(milliseconds) << 80) | u128::from_be_bytes(entropy);
    let alphabet = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let mut output = [b'0'; 26];
    for byte in output.iter_mut().rev() {
        *byte = alphabet[(value & 31) as usize];
        value >>= 5;
    }
    Ok(RunId::new(std::str::from_utf8(&output).unwrap()).unwrap())
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    struct Fixed(AtomicU64);
    impl Clock for Fixed {
        fn now(&self) -> Timestamp {
            Timestamp::new("2026-10-06T00:00:00Z").unwrap()
        }
        fn elapsed(&self) -> Duration {
            Duration::from_secs(self.0.load(Ordering::Relaxed))
        }
    }
    #[test]
    fn excessive_expiry_is_bounded_by_two_equal_validator_observations() {
        let parameters = StoreParameters::default();
        let clock = Fixed(AtomicU64::new(0));
        let expiry = Timestamp::new("2099-10-06T00:00:00Z").unwrap();
        let a = Validator::new("a").unwrap();
        let b = Validator::new("b").unwrap();
        let mut observer = ExpiryObservation::default();
        assert!(!observer.expired(&a, &expiry, &clock, &parameters));
        clock.0.store(929, Ordering::Relaxed);
        assert!(!observer.expired(&a, &expiry, &clock, &parameters));
        clock.0.store(930, Ordering::Relaxed);
        assert!(observer.expired(&a, &expiry, &clock, &parameters));
        assert!(!observer.expired(&b, &expiry, &clock, &parameters));
        clock.0.store(1860, Ordering::Relaxed);
        assert!(observer.expired(&b, &expiry, &clock, &parameters));
    }
    #[test]
    fn lease_expiry_rejects_unrepresentable_years_without_panicking() {
        let now = Timestamp::new("9999-12-31T23:59:59Z").unwrap();
        assert_eq!(
            expires(&now, 60, &StoreParameters::default())
                .unwrap_err()
                .code,
            ErrorCode::InvalidArgument
        );
        let parameters = StoreParameters {
            max_lease_ttl_seconds: grv_storage::model::Counter::new(i64::MAX as u64).unwrap(),
            ..Default::default()
        };
        assert_eq!(
            expires(
                &Timestamp::new("2026-10-06T00:00:00Z").unwrap(),
                i64::MAX as u64,
                &parameters
            )
            .unwrap_err()
            .code,
            ErrorCode::InvalidArgument
        );
    }
    #[test]
    fn run_ids_have_the_exact_ulid_timestamp_and_random_tail() {
        let now = Timestamp::new("2026-10-06T00:00:00Z").unwrap();
        let a = new_run_id(&now).unwrap();
        let b = new_run_id(&now).unwrap();
        assert_eq!(&a.as_str()[..10], &b.as_str()[..10]);
        assert_ne!(a, b);
    }
}
