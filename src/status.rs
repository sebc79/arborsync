use std::future::pending;
use std::time::{Duration, Instant};

pub struct Clock {
    last: Instant,
}

impl Clock {
    pub fn new() -> Self {
        Self {
            last: Instant::now(),
        }
    }

    pub async fn wait(&mut self, seconds: u64) {
        if seconds == 0 {
            pending::<()>().await;
            return;
        }
        let due = self.last + Duration::from_secs(seconds);
        let now = Instant::now();
        if due > now {
            tokio::time::sleep(due - now).await;
        }
        self.last = Instant::now();
    }
}
