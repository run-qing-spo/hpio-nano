use crate::memory_pool::{SlabGuard, SlabPool};
use crate::rate_limiter::TokenBucket;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::Duration;

pub struct WorkerConfig {
    pub id: usize,
    pub total_bytes: u64,
    pub pool: Arc<SlabPool>,
    pub tx: SyncSender<SlabGuard>,
    pub rate_limiter: Arc<TokenBucket>,
}

const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ACQUIRE_TIMEOUTS: u32 = 6;

pub fn run_worker(config: WorkerConfig) {
    let chunk_size = config.pool.slab_size() as u64;
    // 依赖 main 侧对 bytes_per_worker % chunk_size == 0 的校验。
    let mut produced: u64 = 0;
    let mut consecutive_timeouts: u32 = 0;
    let mut chunk_seq: u64 = 0;

    while produced < config.total_bytes {
        let guard = match config.pool.acquire(ACQUIRE_TIMEOUT) {
            Some(g) => {
                consecutive_timeouts = 0;
                g
            }
            None => {
                consecutive_timeouts += 1;
                eprintln!(
                    "worker {}: pool acquire timeout ({}/{})",
                    config.id, consecutive_timeouts, MAX_ACQUIRE_TIMEOUTS
                );
                if consecutive_timeouts >= MAX_ACQUIRE_TIMEOUTS {
                    eprintln!(
                        "worker {}: giving up after {} timeouts (produced {}/{} bytes)",
                        config.id, consecutive_timeouts, produced, config.total_bytes
                    );
                    return;
                }
                continue;
            }
        };

        // 限速放在 worker：不阻塞 main 的 CQ 收割循环。
        config.rate_limiter.acquire(guard.len as u64);

        // 填充缓冲区，用于验证（tag 仅作调试标记，低 4 位会循环）。
        let tag = ((config.id as u8) << 4) | ((chunk_seq as u8) & 0x0F);
        unsafe {
            std::ptr::write_bytes(guard.ptr, tag, guard.len);
        }

        if config.tx.send(guard).is_err() {
            break; // 接收者被丢弃，关闭
        }

        produced += chunk_size;
        chunk_seq += 1;
    }
}
