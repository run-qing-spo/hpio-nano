# hpio-lab / checkpoint_writer

A high-throughput checkpoint writer built on **io_uring + O_DIRECT + 页对齐 slab pool**，用来在 Linux 上把多线程产生的大块数据顺序写入单个文件，并给出 P50/P90/P99/P999 延迟统计。

> 适合的场景：训练框架的 checkpoint dump、日志/状态机批量持久化、benchmark 不同 IO 模式下磁盘的极限吞吐与尾延迟。

---

## 架构

```
                +-----------+    SyncSender<SlabGuard>    +---------------------+
   worker_0 --->|           | --------------------------> |                     |
   worker_1 --->| SlabPool  |                             |  main (single)      |
   worker_2 --->| (page-    |       (有界通道做背压)       |  io_uring reactor   | --> file (O_DIRECT)
   worker_N --->| aligned)  |                             |  顺序分配 offset     |
                +-----------+ <-------------------------- |  收割 CQE -> drop    |
                              SlabGuard 在 IO 完成后 drop  +---------------------+
```

---

## 功能特性

- O_DIRECT 顺序写大文件，绕过页缓存
- `fallocate` 预分配，避免运行时碎片
- 可配置的 token bucket 限速
- HDR 直方图记录写延迟（P50/P90/P99/P999/Max）
- 严格的启动期参数校验：`chunk_size` 对齐、`bytes_per_worker` 对齐、池/环/通道容量一致性

---

## 构建

依赖：Linux 5.6+（io_uring）、Rust 2024 edition。

```bash
cargo build --release
```

输出二进制位于 `target/release/checkpoint_writer`。

---

## 快速上手

默认配置（4 个 worker × 256 MB = 1 GB 数据，128 KB 块，O_DIRECT）：

```bash
cargo run --release
```

指定输出文件、worker 数和总数据量：

```bash
cargo run --release -- \
    --output /tmp/checkpoint.dat \
    --num-workers 8 \
    --bytes-per-worker 268435456
```

带限速（200 MB/s）跑：

```bash
cargo run --release -- --rate-limit $((200 * 1024 * 1024))
```

---

## CLI 选项

| 参数 | 简写 | 默认值 | 说明 |
|---|---|---|---|
| `--output` | `-o` | `checkpoint.dat` | 输出文件路径 |
| `--num-workers` | `-w` | `4` | 工作者线程数 |
| `--chunk-size` | `-c` | `131072` | 每次写入块大小（必须是 4096 的倍数） |
| `--pool-size` | `-p` | `192` | slab pool 中的缓冲区数量 |
| `--ring-depth` | `-d` | `64` | io_uring 队列深度 |
| `--channel-cap` |   | `0`（自动） | worker→main 通道容量；0 表示 `max(num_workers*2, 8)` |
| `--rate-limit` | `-r` | `0` | 限速字节/秒，0 = 不限速 |
| `--bytes-per-worker` | `-b` | `268435456` | 每个 worker 写入的总字节数 |
| `--direct` |   | `true` | 启用 O_DIRECT |

启动时会打印实际生效的配置，例如：

```
=== Checkpoint Writer ===
Workers:     4
Chunk size:  128 KB
Pool size:   192 buffers (24 MB)
Ring depth:  64
Channel cap: 8
Rate limit:  unlimited MB/s
Total write: 1024 MB
Direct IO:   true
```

---

## 容量调优

为避免 worker 在 `pool.acquire` 上长时间排队，pool_size 必须**同时覆盖**三个并发占用：

```
pool_size  >=  ring_depth  +  channel_cap  +  num_workers
            ^               ^                 ^
            |               |                 └── worker 准备下一块时手里持有 1 个
            |               └── 通道排队中的 guard
            └── io_uring 在途的 inflight buffers
```

启动期会强校验该不等式；不满足时直接 bail：

```
Error: pool_size (10) is too small: need at least
       ring_depth (64) + channel_cap (8) + num_workers (4) = 76
```

经验值：

- 想最大化吞吐：`ring_depth` 64–256；`pool_size` 取上式 1.5–2 倍留余量。
- 想压低尾延迟：减小 `channel_cap` 与 `ring_depth`，让背压更早传到 worker。
- 测限速行为：把 `--rate-limit` 设得远低于裸速，观察 P99 是否仍稳定。

---

## 输出统计示例

```
=== Checkpoint Writer Stats ===
Duration:    154.48ms
Total bytes: 0.25 GB
Total ops:   2048
Throughput:  1.62 GB/s
--- Write Latency ---
  P50:  645 us
  P90:  2503 us
  P99:  19567 us
  P999: 19983 us
  Max:  19983 us
```

延迟统计基于 [HDR Histogram](https://hdrhistogram.org/)）

---


## 已知限制

- 仅 Linux（依赖 io_uring 与 `O_DIRECT`）。
- `bytes_per_worker` 必须是 `chunk_size` 的整数倍（启动期校验）。
- 多 worker 写入的"逻辑顺序"由 main 接收顺序决定，因此文件中的块布局与 worker tag 无对应关系；当前不做内容校验，tag 仅作调试标记。
- 不支持运行中扩缩容线程数。
