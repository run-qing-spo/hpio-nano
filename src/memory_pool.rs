use crossbeam_queue::ArrayQueue;
use std::alloc::{alloc, dealloc, Layout};
use std::sync::Arc;

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

    /// 获取一块缓冲区，忙等直到有缓冲区可用
    pub fn acquire(self: &Arc<Self>) -> SlabGuard {
        loop {
            if let Some(index) = self.free_list.pop() {
                return SlabGuard {
                    pool: Arc::clone(self),
                    index,
                    ptr: self.slabs[index],
                    len: self.slab_size,
                };
            }
            // spin 比 sleep/condvar 唤醒更低延迟
            std::hint::spin_loop(); // 告知cpu此时正在等待，优化功耗和性能
        }
    }
    fn release(&self, index: usize) {
        if let Err(_) = self.free_list.push(index) {
            // release 模式下也能感知到异常，但不会 panic
            eprintln!("BUG: free list overflow (index {})", index);
        }
    }
    pub fn slab_size(&self) -> usize {
        self.slab_size
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