use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct TokenBucket {
    inner: Mutex<Inner>, // 限速器不在热路径上（每次 IO 才调一次），锁竞争极低。
    rate: u64,
    max_tokens: u64,
}

struct Inner {
    tokens: u64,
    last_refill: Instant,
}

impl TokenBucket {
    /// rate = 0 表示不限速：这样 benchmark 时可以测"全速"和"限速"两种模式。
    pub fn new(rate: u64) -> Self {
        let max_tokens = if rate == 0 { u64::MAX } else { rate };
        Self {
            inner: Mutex::new(Inner {
                tokens: max_tokens,
                last_refill: Instant::now(),
            }),
            rate,
            max_tokens,
        }
    }

    pub fn acquire(&self, bytes: u64) {
        if self.rate == 0 {
            return;
        }

        let mut remaining = bytes;

        loop {
            let sleep_dur = {
                let mut inner = self.inner.lock().unwrap();
                self.refill(&mut inner);

                if inner.tokens >= remaining {
                    inner.tokens -= remaining;
                    return;
                }

                remaining -= inner.tokens;
                inner.tokens = 0;

                let wait_tokens = remaining.min(self.max_tokens);
                Duration::from_secs_f64(wait_tokens as f64 / self.rate as f64)
            };

            // thread::sleep 在 Linux 上精度约 1-4ms，对于 MB/s 级别的限速足够。
            std::thread::sleep(sleep_dur); 
        }
    }

    fn refill(&self, inner: &mut Inner) {
        let now = Instant::now();
        let elapsed = now.duration_since(inner.last_refill);
        let new_tokens = (elapsed.as_secs_f64() * self.rate as f64) as u64;

        if new_tokens > 0 {
            inner.tokens = (inner.tokens + new_tokens).min(self.max_tokens);
            // 只前进实际产出整数令牌所对应的时间,
            // 保留小数余量给下次 refill，保证长期速率精确匹配目标值.
            let consumed = Duration::from_secs_f64(new_tokens as f64 / self.rate as f64);
            inner.last_refill += consumed;
        }
    }
}