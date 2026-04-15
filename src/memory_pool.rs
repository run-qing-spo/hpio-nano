use crossbeam_queue::ArrayQueue;
use std::alloc::{alloc, dealloc, Layout};
use std::sync::Arc;
use std::time::Duration;

pub struct SlabPool{
    slabs: Vec<*mut u8>,
    free_list: ArrayQueue<usize>,
    slab_size: usize,
    layout: Layout,
}

unsafe impl Send for SlabPool {}
unsafe impl Sync for SlabPool {}

impl SlabPool {
    pub fn new(count: usize, slab_size: usize) -> Self {
        // Direct I/O 场景，页对齐
        let layout = Layout::from_size_align(slab_size, 4096)
            .expect("invalid layout");
        let mut slabs = Vec::with_capacity(count);
        let free_list = ArrayQueue::new(count);
        for i in 0..count {
            let ptr = unsafe { alloc(layout) };
            assert!(!ptr.is_null(), "allocation failed for slab {}", i);
            slabs.push(ptr);
            free_list.push(i).unwrap();
        }
        Self { slabs, free_list, slab_size, layout }
    }

    /// Linux 内核中 mutex_spin_on_owner 的思路类似
    pub fn acquire(self: &Arc<Self>, timeout: Duration) -> Option<SlabGuard> {
        let start = std::time::Instant::now();
        // 第一级：纯自旋，最低延迟
        for _ in 0..64 {
            if let Some(guard) = self.try_acquire() {
                return Some(guard);
            }
            // spin 比 sleep/condvar 唤醒更低延迟
            std::hint::spin_loop(); // 告知cpu此时正在等待，优化功耗和性能
        }

        // 第二级：yield，让出时间片给同核的其他线程
        for _ in 0..32 {
            if let Some(guard) = self.try_acquire() {
                return Some(guard);
            }
            std::thread::yield_now();
        }

        // 第三级：sleep，彻底让出 CPU
        loop {
            if let Some(guard) = self.try_acquire() {
                return Some(guard);
            }
            std::thread::sleep(Duration::from_micros(10));
            if start.elapsed() >= timeout {
                return None;
            }
        }

    }
    fn release(&self, index: usize) {
        let push_result = self.free_list.push(index);

        // 开发期：debug_assert 强制暴露 bug
        debug_assert!(
            push_result.is_ok(),
            "BUG: free list overflow (index {}), possible double-free",
            index
        );

        // 生产期：记录日志但不 panic
        if let Err(index) = push_result {
            eprintln!("BUG: free list overflow in release mode, index {} dropped", index);
        }
    }
    pub fn slab_size(&self) -> usize {
        self.slab_size
    }
    fn make_guard(self: &Arc<Self>, index: usize) -> SlabGuard {
        SlabGuard {
            pool: Arc::clone(self),
            index,
            ptr: self.slabs[index],
            len: self.slab_size,
        }
    }

    /// 非阻塞获取，立即返回
    pub fn try_acquire(self: &Arc<Self>) -> Option<SlabGuard> {
        self.free_list.pop().map(|index| self.make_guard(index))
    }
}

impl Drop for SlabPool {
    fn drop(&mut self) {
        for ptr in &self.slabs {
            unsafe { dealloc(*ptr, self.layout); }
        }
    }
}

pub struct SlabGuard {
    pool: Arc<SlabPool>,
    index: usize,
    pub ptr: *mut u8,
    pub len: usize,
}

unsafe impl Send for SlabGuard {}
impl Drop for SlabGuard {
    fn drop(&mut self) {
        self.pool.release(self.index);
    }
}