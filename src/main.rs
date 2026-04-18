mod memory_pool;
mod rate_limiter;
mod stats;
mod worker;
mod writer;

use crate::memory_pool::SlabPool;
use crate::rate_limiter::TokenBucket;
use crate::stats::Stats;
use crate::worker::{run_worker, WorkerConfig};
use crate::writer::UringWriter;

use clap::Parser;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

#[derive(Parser)]
#[command(name = "checkpoint_writer")]
#[command(about = "High-performance checkpoint writer using io_uring")]
struct Args {
    /// 输出路径
    #[arg(short, long, default_value = "checkpoint.dat")]
    output: String,

    /// 线程数量
    #[arg(short = 'w', long, default_value_t = 4)]
    num_workers: usize,

    /// 块大小（必须是4096的倍数）
    #[arg(short, long, default_value_t = 131072)]
    chunk_size: usize,

    /// 内存池中的缓冲区数量
    #[arg(short, long, default_value_t = 64)]
    pool_size: usize,

    /// io_uring 队列深度
    #[arg(short = 'd', long, default_value_t = 64)]
    ring_depth: u32,

    /// 速率限制（字节/秒，0 = 无限制）
    #[arg(short, long, default_value_t = 0)]
    rate_limit: u64,

    /// 每个worker写入的总字节数
    #[arg(short = 'b', long, default_value_t = 268435456)]
    bytes_per_worker: u64,

    /// 使用 O_DIRECT（绕过页缓存）
    #[arg(long, default_value_t = true)]
    direct: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // 验证块大小对齐
    if args.chunk_size % 4096 != 0 {
        anyhow::bail!("chunk_size must be a multiple of 4096");
    }

    println!("=== Checkpoint Writer ===");
    println!("Workers:     {}", args.num_workers);
    println!("Chunk size:  {} KB", args.chunk_size / 1024);
    println!("Pool size:   {} buffers ({} MB)",
        args.pool_size,
        args.pool_size * args.chunk_size / (1024 * 1024));
    println!("Ring depth:  {}", args.ring_depth);
    println!("Rate limit:  {} MB/s",
        if args.rate_limit == 0 { "unlimited".to_string() }
        else { format!("{}", args.rate_limit / (1024 * 1024)) });
    println!("Total write: {} MB",
        args.num_workers as u64 * args.bytes_per_worker / (1024 * 1024));
    println!("Direct IO:   {}", args.direct);
    println!();

    // 打开输出文件
    let mut open_opts = OpenOptions::new();
    open_opts.write(true).create(true).truncate(true);
    if args.direct {
        open_opts.custom_flags(libc::O_DIRECT);
    }
    let file = open_opts.open(&args.output)?;
    let fd = file.as_raw_fd();

    // 预分配文件大小以避免碎片化
    let total_bytes = args.num_workers as u64 * args.bytes_per_worker;
    unsafe { libc::fallocate(fd, 0, 0, total_bytes as i64); }

    // 初始化组件
    let pool = Arc::new(SlabPool::new(args.pool_size, args.chunk_size));
    let rate_limiter = TokenBucket::new(args.rate_limit);
    let stats = Stats::new();
    let mut writer = UringWriter::new(fd, args.ring_depth)?;

    // 有界通道：容量 = ring_depth，提供背压
    let (tx, rx) = mpsc::sync_channel(args.ring_depth as usize);

    // 启动工作者线程
    let mut handles = Vec::new();
    for i in 0..args.num_workers {
        let config = WorkerConfig {
            id: i,
            total_bytes: args.bytes_per_worker,
            pool: Arc::clone(&pool),
            tx: tx.clone(),
        };
        handles.push(thread::spawn(move || run_worker(config)));
    }
    drop(tx); // 关闭发送者，使 rx 迭代器在所有工作者完成后结束

    // 主写循环
    let mut inflight_bufs: HashMap<u64, memory_pool::SlabGuard> = HashMap::new();
    let mut seq: u64 = 0;
    let mut offset: u64 = 0;

    use std::sync::mpsc::TryRecvError;
use std::time::Duration;

let mut done = false;
while !done {
    // 尝试接收一个缓冲区（非阻塞）
    match rx.try_recv() {
        Ok(guard) => {
            rate_limiter.acquire(guard.len as u64);
            let completions = writer.submit_write(
                guard.ptr, guard.len as u32, offset, seq
            )?;
            inflight_bufs.insert(seq, guard);

            for c in completions {
                if let Some(_released) = inflight_bufs.remove(&c.user_data) {
                    stats.record_write(c.latency_us, c.bytes_written as u64);
                }
            }

            seq += 1;
            offset += args.chunk_size as u64;
        }
        Err(TryRecvError::Disconnected) => {
            done = true; // 所有worker完成
        }
        Err(TryRecvError::Empty) => {
            // 通道空 — worker可能正在等待缓冲区
            // 不要忙等，短暂让出时间片
            if inflight_bufs.is_empty() {
                std::thread::sleep(Duration::from_micros(10));
            }
        }
    }

    // 总是drain completions，无论recv结果如何
    // 避免死锁循环
    for c in writer.reap_completions()? {
        if let Some(_released) = inflight_bufs.remove(&c.user_data) {
            stats.record_write(c.latency_us, c.bytes_written as u64);
        }
    }
}

    // flush剩余的inflight IO
    for c in writer.flush()? {
        if let Some(_released) = inflight_bufs.remove(&c.user_data) {
            stats.record_write(c.latency_us, c.bytes_written as u64);
        }
    }

    // 等待所有worker完成
    for h in handles {
        h.join().expect("worker thread panicked");
    }

    // 同步到磁盘
    file.sync_all()?;

    // 打印统计信息
    stats.report();

    println!("\nCheckpoint written to: {}", args.output);
    Ok(())
}