//! A client-side cap on broker calls, shared by every plane in the process.
//! Composio counts every authenticated call against one per-org budget in a
//! fixed one-minute window (Hobby 2,000, Pro 10,000).
use std::{collections::VecDeque, sync::Mutex, time::Duration};
use tokio::time::Instant;

pub const DEFAULT_CALLS_PER_MINUTE: usize = 1_500;

pub struct CallBudget {
    limit: usize,
    window: Duration,
    sent: Mutex<VecDeque<Instant>>,
}
impl CallBudget {
    pub fn new(limit: usize, window: Duration) -> Self {
        let limit = limit.max(1);
        Self {
            limit,
            window,
            sent: Mutex::new(VecDeque::with_capacity(limit)),
        }
    }
    pub fn per_minute(limit: usize) -> Self {
        Self::new(limit, Duration::from_secs(60))
    }
    /// `COMPOSIO_CALLS_PER_MINUTE`, or the default when unset or not a
    /// positive number.
    pub fn from_env() -> Self {
        let limit = std::env::var("COMPOSIO_CALLS_PER_MINUTE")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_CALLS_PER_MINUTE);
        Self::per_minute(limit)
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
    /// Waits until fewer than `limit` calls went out in the trailing window,
    /// then counts this one. Capping every trailing window also caps every
    /// fixed window, wherever Composio's minute boundaries fall.
    pub async fn acquire(&self) {
        loop {
            let wait = {
                let mut sent = self.sent.lock().unwrap_or_else(|e| e.into_inner());
                let now = Instant::now();
                while sent
                    .front()
                    .is_some_and(|t| now.duration_since(*t) >= self.window)
                {
                    sent.pop_front();
                }
                if sent.len() < self.limit {
                    sent.push_back(now);
                    return;
                }
                self.window - now.duration_since(sent[0])
            };
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test(start_paused = true)]
    async fn no_window_ever_holds_more_than_the_limit() {
        let budget = Arc::new(CallBudget::new(50, Duration::from_secs(60)));
        let start = Instant::now();
        let stamps: Vec<Duration> = futures::future::join_all((0..175).map(|_| {
            let budget = budget.clone();
            async move {
                budget.acquire().await;
                start.elapsed()
            }
        }))
        .await;
        let mut sorted = stamps.clone();
        sorted.sort();
        for (i, t) in sorted.iter().enumerate() {
            let in_window = sorted[i..]
                .iter()
                .take_while(|u| **u - *t < Duration::from_secs(60))
                .count();
            assert!(in_window <= 50, "{in_window} calls within 60s of {t:?}");
        }
        assert_eq!(sorted.iter().filter(|t| **t < Duration::from_secs(60)).count(), 50);
        assert_eq!(*sorted.last().unwrap(), Duration::from_secs(180), "175 calls need four windows");
    }
}
