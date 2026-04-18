use crate::memory_pool::{SlabGuard, SlabPool};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::Duration;

pub struct WorkerConfig {
    pub id: usize,
    pub total_bytes: u64,
    pub pool: Arc<SlabPool>,
    pub tx: SyncSender<SlabGuard>,
}

pub fn run_worker(config: WorkerConfig) {
    let chunk_size = config.pool.slab_size() as u64;
    let mut produced: u64 = 0;

    while produced < config.total_bytes {
        let guard = match config.pool.acquire(Duration::from_secs(5)) {
            Some(g) => g,
            None => {
                eprintln!("worker {}: pool acquire timeout, retrying", config.id);
                continue;
            }
        };

        // 填充缓冲区，用于验证
        let tag = ((config.id as u8) << 4) | ((produced / chunk_size) as u8 & 0x0F);
        unsafe {
            std::ptr::write_bytes(guard.ptr, tag, guard.len);
        }

        if config.tx.send(guard).is_err() {
            break; // 接收者被丢弃，关闭
        }

        produced += chunk_size;
    }
}