mod memory_pool;
mod rate_limiter;
mod stats;
mod worker;
mod writer;

use crate::memory_pool::{SlabGuard, SlabPool};
use crate::rate_limiter::TokenBucket;
use crate::stats::Stats;
use crate::worker::{run_worker, WorkerConfig};
use crate::writer::{UringWriter, WriteCompletion};

use clap::Parser;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::sync::mpsc::{self, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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
    #[arg(short, long, default_value_t = 192)]
    pool_size: usize,

    /// io_uring 队列深度
    #[arg(short = 'd', long, default_value_t = 64)]
    ring_depth: u32,

    /// worker→main 通道的容量（0 表示自动：num_workers*2，至少 8）
    #[arg(long, default_value_t = 0)]
    channel_cap: usize,

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

    // ---- 参数校验 ----
    if args.chunk_size % 4096 != 0 {
        anyhow::bail!("chunk_size must be a multiple of 4096");
    }
    if args.bytes_per_worker % args.chunk_size as u64 != 0 {
        anyhow::bail!(
            "bytes_per_worker ({}) must be a multiple of chunk_size ({})",
            args.bytes_per_worker, args.chunk_size
        );
    }
    if args.num_workers == 0 {
        anyhow::bail!("num_workers must be > 0");
    }
    if args.ring_depth == 0 {
        anyhow::bail!("ring_depth must be > 0");
    }

    let channel_cap = if args.channel_cap == 0 {
        (args.num_workers * 2).max(8)
    } else {
        args.channel_cap
    };

    // pool 必须同时覆盖：ring 在途 + channel 排队 + 每个 worker 在手上的下一块。
    let required_pool = args.ring_depth as usize + channel_cap + args.num_workers;
    if args.pool_size < required_pool {
        anyhow::bail!(
            "pool_size ({}) is too small: need at least ring_depth ({}) + channel_cap ({}) + num_workers ({}) = {}",
            args.pool_size, args.ring_depth, channel_cap, args.num_workers, required_pool
        );
    }

    println!("=== Checkpoint Writer ===");
    println!("Workers:     {}", args.num_workers);
    println!("Chunk size:  {} KB", args.chunk_size / 1024);
    println!(
        "Pool size:   {} buffers ({} MB)",
        args.pool_size,
        args.pool_size * args.chunk_size / (1024 * 1024)
    );
    println!("Ring depth:  {}", args.ring_depth);
    println!("Channel cap: {}", channel_cap);
    println!(
        "Rate limit:  {} MB/s",
        if args.rate_limit == 0 {
            "unlimited".to_string()
        } else {
            format!("{}", args.rate_limit / (1024 * 1024))
        }
    );
    println!(
        "Total write: {} MB",
        args.num_workers as u64 * args.bytes_per_worker / (1024 * 1024)
    );
    println!("Direct IO:   {}", args.direct);
    println!();

    // ---- 打开输出文件 ----
    let mut open_opts = OpenOptions::new();
    open_opts.write(true).create(true).truncate(true);
    if args.direct {
        open_opts.custom_flags(libc::O_DIRECT);
    }
    let file = open_opts.open(&args.output)?;
    let fd = file.as_raw_fd();

    // ---- 预分配文件 ----
    let total_bytes = args.num_workers as u64 * args.bytes_per_worker;
    let ret = unsafe { libc::fallocate(fd, 0, 0, total_bytes as i64) };
    if ret != 0 {
        // 某些 FS 不支持 fallocate（如 tmpfs）。这里选择硬失败以暴露配置问题，
        // 而不是静默让 O_DIRECT 写入进入未预分配区间。
        return Err(io::Error::last_os_error().into());
    }

    // ---- 初始化组件 ----
    let pool = Arc::new(SlabPool::new(args.pool_size, args.chunk_size));
    let rate_limiter = Arc::new(TokenBucket::new(args.rate_limit));
    let stats = Stats::new();
    let mut writer = UringWriter::new(fd, args.ring_depth)?;

    let (tx, rx) = mpsc::sync_channel::<SlabGuard>(channel_cap);

    // ---- 启动工作者线程 ----
    let mut handles = Vec::new();
    for i in 0..args.num_workers {
        let config = WorkerConfig {
            id: i,
            total_bytes: args.bytes_per_worker,
            pool: Arc::clone(&pool),
            tx: tx.clone(),
            rate_limiter: Arc::clone(&rate_limiter),
        };
        handles.push(thread::spawn(move || run_worker(config)));
    }
    drop(tx); // 保证所有 worker 退出后 rx 会 Disconnected。

    // ---- 主写循环 ----
    let mut inflight_bufs: HashMap<u64, SlabGuard> = HashMap::new();
    let mut seq: u64 = 0;
    let mut offset: u64 = 0;

    let handle_completion = |c: WriteCompletion,
                             bufs: &mut HashMap<u64, SlabGuard>| {
        if let Some(_released) = bufs.remove(&c.user_data) {
            stats.record_write(c.latency_us, c.bytes_written as u64);
        }
    };

    loop {
        match rx.try_recv() {
            Ok(guard) => {
                let completions = writer.submit_write(
                    guard.ptr,
                    guard.len as u32,
                    offset,
                    seq,
                )?;
                inflight_bufs.insert(seq, guard);
                for c in completions {
                    handle_completion(c, &mut inflight_bufs);
                }
                // 机会式非阻塞收割，尽量让 buffer 早回池。
                for c in writer.reap_completions()? {
                    handle_completion(c, &mut inflight_bufs);
                }

                seq += 1;
                offset += args.chunk_size as u64;
            }
            Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {
                if writer.inflight() > 0 {
                    // 有在途 IO：阻塞等至少一个 CQE，避免空转。
                    for c in writer.wait_completions()? {
                        handle_completion(c, &mut inflight_bufs);
                    }
                } else {
                    // 无在途 IO：worker 正在准备，短暂让出时间片。
                    thread::sleep(Duration::from_micros(100));
                }
            }
        }
    }

    // ---- flush 剩余在途 IO ----
    for c in writer.flush()? {
        handle_completion(c, &mut inflight_bufs);
    }

    // ---- 等待 worker 全部退出 ----
    for h in handles {
        h.join().expect("worker thread panicked");
    }

    // ---- 同步到磁盘 ----
    file.sync_all()?;

    stats.report();

    println!("\nCheckpoint written to: {}", args.output);
    Ok(())
}
