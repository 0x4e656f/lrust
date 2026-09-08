//! SQLx-only reconnect policy. No timers, background retries or SQL replay.
use std::time::{Duration, Instant};

pub(super) struct ReconnectBackoff {
    initial: Duration,
    max: Duration,
    delay: Duration,
    retry_at: Option<Instant>,
}

impl ReconnectBackoff {
    pub(super) fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            delay: Duration::ZERO,
            retry_at: None,
        }
    }

    pub(super) fn failed(&mut self, now: Instant) {
        self.delay = if self.delay.is_zero() {
            self.initial
        } else {
            self.delay.saturating_mul(2).min(self.max)
        };
        self.retry_at = Some(now + self.delay);
    }

    pub(super) fn reset(&mut self) {
        self.delay = Duration::ZERO;
        self.retry_at = None;
    }

    pub(super) fn retry_after_ms(&self, now: Instant) -> u64 {
        self.retry_at.map_or(0, |at| {
            at.saturating_duration_since(now)
                .as_nanos()
                .div_ceil(1_000_000) as u64
        })
    }
}

pub(super) struct ConnectionLogGate {
    interval: Duration,
    next_log: Option<Instant>,
    suppressed: u64,
}

impl ConnectionLogGate {
    pub(super) fn new(interval: Duration) -> Self {
        Self {
            interval,
            next_log: None,
            suppressed: 0,
        }
    }

    // Only used for fire-and-forget connection failures. Every waiting caller
    // still receives its own error; ordinary SQL errors are never suppressed.
    pub(super) fn take(&mut self, now: Instant) -> Option<u64> {
        if self.next_log.is_some_and(|at| now < at) {
            self.suppressed = self.suppressed.saturating_add(1);
            return None;
        }
        self.next_log = Some(now + self.interval);
        Some(std::mem::take(&mut self.suppressed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_cap_expiration_and_recovery() {
        let now = Instant::now();
        let mut retry =
            ReconnectBackoff::new(Duration::from_millis(250), Duration::from_millis(1000));
        assert_eq!(retry.retry_after_ms(now), 0);
        for delay in [250, 500, 1000, 1000] {
            retry.failed(now);
            assert_eq!(retry.retry_after_ms(now), delay);
            assert_eq!(retry.retry_after_ms(now + Duration::from_millis(delay)), 0);
        }
        retry.reset();
        assert_eq!(retry.retry_after_ms(now), 0);
        retry.failed(now);
        assert_eq!(retry.retry_after_ms(now + Duration::from_micros(1)), 250);
        assert_eq!(retry.retry_after_ms(now + Duration::from_millis(249)), 1);
    }

    #[test]
    fn disabled_backoff_and_independent_connections() {
        let now = Instant::now();
        let mut disabled = ReconnectBackoff::new(Duration::ZERO, Duration::from_secs(5));
        let mut other = ReconnectBackoff::new(Duration::from_millis(250), Duration::from_secs(5));
        for _ in 0..100 {
            disabled.failed(now);
            assert_eq!(disabled.retry_after_ms(now), 0);
        }
        other.failed(now);
        assert_eq!(disabled.retry_after_ms(now), 0);
        assert_eq!(other.retry_after_ms(now), 250);
    }

    #[test]
    fn connection_logs_are_counted_and_rate_limited() {
        let now = Instant::now();
        let mut gate = ConnectionLogGate::new(Duration::from_secs(5));
        assert_eq!(gate.take(now), Some(0));
        for _ in 0..100 {
            assert_eq!(gate.take(now), None);
        }
        assert_eq!(gate.take(now + Duration::from_secs(5)), Some(100));
        assert_eq!(gate.take(now + Duration::from_secs(10)), Some(0));
        let mut disabled = ConnectionLogGate::new(Duration::ZERO);
        for _ in 0..100 {
            assert_eq!(disabled.take(now), Some(0));
        }
    }
}
