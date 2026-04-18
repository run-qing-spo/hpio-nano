use io_uring::{opcode, types, IoUring};
use std::io;
use std::os::unix::io::RawFd;
use std::time::Instant;

pub struct WriteCompletion {
    pub user_data: u64,
    pub bytes_written: u32,
    pub latency_us: u64,
}

pub struct UringWriter {
    ring: IoUring,
    fd: RawFd,
    inflight: u32,
    max_inflight: u32,
    submit_times: Vec<Option<Instant>>, // user_data 递增场景够用
}

impl UringWriter {
    pub fn new(fd: RawFd, queue_depth: u32) -> io::Result<Self> {
        let ring = IoUring::new(queue_depth)?;
        let submit_times = vec![None; queue_depth as usize];
        Ok(Self {
            ring,
            fd,
            inflight: 0,
            max_inflight: queue_depth,
            submit_times,
        })
    }

    /// 提交一个写请求。如果环已满，先阻塞等至少一个 CQE 再提交，
    /// 从而保证 SQ 永远有空位，不会走到 "SQ full" 这个错误路径。
    /// 返回在腾空间过程中收割到的完成项。
    pub fn submit_write(
        &mut self,
        buf: *const u8,
        len: u32,
        offset: u64,
        user_data: u64,
    ) -> io::Result<Vec<WriteCompletion>> {
        let mut completions = Vec::new();

        // 环已满：必须先收割。为了消除"CQ 暂时为空 → push 返回 SQ full"
        // 这一竞态，使用 submit_and_wait(1) 阻塞至少一个完成项到达。
        if self.inflight >= self.max_inflight {
            self.ring.submit_and_wait(1)?;
            self.drain_completions(&mut completions)?;
        }

        let slot = (user_data as usize) % self.submit_times.len();
        self.submit_times[slot] = Some(Instant::now());

        let sqe = opcode::Write::new(types::Fd(self.fd), buf, len)
            .offset(offset)
            .build()
            .user_data(user_data);

        unsafe {
            self.ring
                .submission()
                .push(&sqe)
                .map_err(|_| io::Error::other("SQ full"))?;
        }

        self.ring.submit()?;
        self.inflight += 1; // 当前每次 push 1 个就 submit ()

        Ok(completions)
    }

    /// 非阻塞地收割所有可用的完成项。
    pub fn reap_completions(&mut self) -> io::Result<Vec<WriteCompletion>> {
        let mut completions = Vec::new();
        self.drain_completions(&mut completions)?;
        Ok(completions)
    }

    /// 阻塞至少等待一个 CQE 到达（当有在途 IO 时），然后一次性收割所有可用完成项。
    /// 供 main 在 "通道暂时没东西但仍有 inflight" 场景下使用，避免忙等。
    /// 若没有 inflight，立刻返回空向量（不阻塞）。
    pub fn wait_completions(&mut self) -> io::Result<Vec<WriteCompletion>> {
        let mut completions = Vec::new();
        if self.inflight == 0 {
            return Ok(completions);
        }
        self.ring.submit_and_wait(1)?;
        self.drain_completions(&mut completions)?;
        Ok(completions)
    }

    pub fn inflight(&self) -> u32 {
        self.inflight
    }

    /// 等待所有在途 IO 完成。
    pub fn flush(&mut self) -> io::Result<Vec<WriteCompletion>> {
        let mut completions = Vec::new();
        while self.inflight > 0 {
            self.ring.submit_and_wait(1)?;
            self.drain_completions(&mut completions)?;
        }
        Ok(completions)
    }

    fn drain_completions(
        &mut self,
        out: &mut Vec<WriteCompletion>,
    ) -> io::Result<()> {
        let cq = self.ring.completion();
        for cqe in cq {
            let ud = cqe.user_data();
            let res = cqe.result();

            // CQE 的 result 字段就是 write() 系统调用的返回值。
            // 负数是 -errno。
            if res < 0 {
                return Err(io::Error::from_raw_os_error(-res));
            }

            let slot = (ud as usize) % self.submit_times.len();
            let latency_us = self.submit_times[slot]
                .take()
                .map(|t| t.elapsed().as_micros() as u64)
                .unwrap_or(0);

            out.push(WriteCompletion {
                user_data: ud,
                bytes_written: res as u32,
                latency_us,
            });
            self.inflight -= 1;
        }
        Ok(())
    }
}