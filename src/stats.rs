use hdrhistogram::Histogram;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

pub struct Stats {
    write_latency_us: Mutex<Histogram<u64>>,
    bytes_written: AtomicU64,
    ops_completed: AtomicU64,
    start_time: Instant,
}

impl Stats {
    /// 创建一个新的 Stats 实例，初始化 HDR 直方图（范围 1~10,000,000 μs，精度 3 位有效数字）
    /// 这是工业界标准的延迟统计方法（比求平均值有意义得多）
    /// 以及归零的字节/操作计数器，并记录起始时刻。
    pub fn new() -> Self {
        Self {
            write_latency_us: Mutex::new(
                Histogram::new_with_bounds(1, 10_000_000, 3).unwrap()
            ),
            bytes_written: AtomicU64::new(0),
            ops_completed: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    /// 记录一次写操作的延迟（微秒）和写入字节数。
    /// 将延迟采样到直方图中，并原子地累加总字节数与操作数。
    pub fn record_write(&self, latency_us: u64, bytes: u64) {
        self.write_latency_us.lock().unwrap()
            .record(latency_us).ok();
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.ops_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// 打印汇总报告：运行时长、总写入量（GB）、操作次数、吞吐量（GB/s），
    /// 以及写延迟的 P50/P90/P99/P999 和最大值（微秒）。
    pub fn report(&self) {
        let elapsed = self.start_time.elapsed();
        let total_bytes = self.bytes_written.load(Ordering::Relaxed);
        let total_ops = self.ops_completed.load(Ordering::Relaxed);
        let hist = self.write_latency_us.lock().unwrap();

        let throughput_gbs = total_bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0 * 1024.0);

        println!("=== Checkpoint Writer Stats ===");
        println!("Duration:    {:.2?}", elapsed);
        println!("Total bytes: {:.2} GB", total_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
        println!("Total ops:   {}", total_ops);
        println!("Throughput:  {:.2} GB/s", throughput_gbs);
        println!("--- Write Latency ---");
        println!("  P50:  {} us", hist.value_at_quantile(0.50));
        println!("  P90:  {} us", hist.value_at_quantile(0.90));
        println!("  P99:  {} us", hist.value_at_quantile(0.99));
        println!("  P999: {} us", hist.value_at_quantile(0.999));
        println!("  Max:  {} us", hist.max());
    }
}