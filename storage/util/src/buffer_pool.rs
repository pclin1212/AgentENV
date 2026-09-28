//! Fixed-size buffer recycling for compact writers.
//!
//! [`CompactWriter::alloc_buffer`](crate::compact_writer::CompactWriter::alloc_buffer)
//! runs once per data chunk during a compact/commit pass. Handing out a fresh
//! large allocation each time forces every page of every chunk through
//! first-touch faults (the memory-snapshot fill path is a synchronous
//! `process_vm_readv`, so those faults land in kernel mode inside the
//! syscall) and keeps the allocator churning multi-hundred-KiB extents
//! across threads. These types recycle one small set of fixed-size buffers
//! for the whole pass instead: pages fault in once and the steady-state
//! allocation count drops to the number of in-flight chunks.
//!
//! [`PooledBuffer`] is the exclusive, mutable fill-side handle returned by
//! `alloc_buffer`; dropping it (including on error paths) recycles it.
//! [`PooledBuffer::into_slice`] converts a filled buffer to the shared,
//! read-only [`SlabSlice`] that consume-side workers hold: slices are
//! zero-copy, cheap to clone and sub-slice, and the backing memory recycles
//! only when the last reference drops.
//!
//! Pools are conversion-scoped by design (they live inside one writer for
//! one compact/commit run) rather than process-global: thousands of chunks
//! within a single run reuse the same few buffers, and the memory returns
//! to the OS when the writer drops.

use std::ops::{Deref, Range};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct FixedBufferPool {
    size: usize,
    cap: usize,
    idle: Mutex<Vec<Box<[u8]>>>,
    allocations: AtomicUsize,
}

impl FixedBufferPool {
    /// Create a pool recycling buffers of exactly `size` bytes and retaining
    /// at most `cap` idle buffers.
    pub fn new(size: usize, cap: usize) -> Arc<Self> {
        Arc::new(Self {
            size,
            cap,
            idle: Mutex::new(Vec::new()),
            allocations: AtomicUsize::new(0),
        })
    }

    /// Buffer size handed out by this pool.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Fresh allocations served since pool creation. Steady state should
    /// hold this at the number of in-flight buffers.
    pub fn allocations(&self) -> usize {
        self.allocations.load(Ordering::Relaxed)
    }

    /// Take a buffer out of the pool, allocating a fresh one when none is
    /// idle. Takes the shared handle because [`PooledBuffer`] keeps a
    /// back-reference so it can recycle itself on drop.
    pub fn acquire(pool: &Arc<Self>) -> PooledBuffer {
        let data = pool.pop().unwrap_or_else(|| {
            pool.allocations.fetch_add(1, Ordering::Relaxed);
            vec![0u8; pool.size].into_boxed_slice()
        });
        PooledBuffer {
            data,
            pool: pool.clone(),
        }
    }

    fn pop(&self) -> Option<Box<[u8]>> {
        self.idle.lock().unwrap().pop()
    }

    fn push(&self, data: Box<[u8]>) {
        if data.len() != self.size {
            // Foreign buffer: let it drop through to the allocator.
            return;
        }
        let mut idle = self.idle.lock().unwrap();
        if idle.len() < self.cap {
            idle.push(data);
        }
        // Over cap: release back to the allocator instead of retaining.
    }
}

/// Exclusive fill-side handle for one pooled buffer.
pub struct PooledBuffer {
    data: Box<[u8]>,
    pool: Arc<FixedBufferPool>,
}

impl PooledBuffer {
    /// Convert the filled buffer into a shared read-only slice covering all
    /// its bytes. The underlying bytes are untouched; only ownership
    /// changes, and the memory recycles when the last reference drops.
    pub fn into_slice(self) -> SlabSlice {
        let len = self.data.len();
        self.into_slice_len(len)
    }

    /// Convert the filled buffer into a shared read-only slice covering its
    /// first `len` bytes. The tail stays unexposed inside the pooled buffer,
    /// which recycles when the last reference drops.
    pub fn into_slice_len(mut self, len: usize) -> SlabSlice {
        let size = self.data.len();
        assert!(
            len <= size,
            "slice len {len} exceeds pooled buffer size {size}"
        );
        // Empty data keeps this value's Drop from recycling the buffer that
        // now lives inside the slice.
        let data = std::mem::take(&mut self.data);
        SlabSlice {
            inner: Arc::new(SlabInner {
                data,
                pool: Some(self.pool.clone()),
            }),
            off: 0,
            len,
        }
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if self.data.len() == self.pool.size {
            let data = std::mem::take(&mut self.data);
            self.pool.push(data);
        }
    }
}

impl AsRef<[u8]> for PooledBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsMut<[u8]> for PooledBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

struct SlabInner {
    data: Box<[u8]>,
    /// `None` for one-off slices built from caller-owned vectors
    /// ([`SlabSlice::from_vec`]); those free normally on drop.
    pool: Option<Arc<FixedBufferPool>>,
}

impl Drop for SlabInner {
    fn drop(&mut self) {
        if let Some(pool) = &self.pool {
            if self.data.len() == pool.size {
                let data = std::mem::take(&mut self.data);
                pool.push(data);
            }
        }
    }
}

/// Zero-copy window into pooled memory (or a caller-owned vector via
/// [`SlabSlice::from_vec`]). Cheap to clone and sub-slice; the backing
/// memory recycles when the last reference drops.
pub struct SlabSlice {
    inner: Arc<SlabInner>,
    off: usize,
    len: usize,
}

impl SlabSlice {
    /// Wrap a caller-owned vector without a pool; dropping the last slice
    /// frees the vector instead of recycling it.
    pub fn from_vec(data: Vec<u8>) -> Self {
        let len = data.len();
        Self {
            inner: Arc::new(SlabInner {
                data: data.into_boxed_slice(),
                pool: None,
            }),
            off: 0,
            len,
        }
    }

    /// Sub-slice sharing the same backing memory.
    pub fn slice(&self, range: Range<usize>) -> Self {
        assert!(
            range.start <= range.end && range.end <= self.len,
            "slab slice {range:?} out of bounds for {len} byte slice",
            len = self.len
        );
        Self {
            inner: self.inner.clone(),
            off: self.off + range.start,
            len: range.len(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Deref for SlabSlice {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.inner.data[self.off..self.off + self.len]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recycles_buffers_across_acquire_drop_cycles() {
        let pool = FixedBufferPool::new(4096, 2);
        assert_eq!(pool.size(), 4096);
        for round in 0u8..10 {
            let mut buffer = FixedBufferPool::acquire(&pool);
            buffer.as_mut().fill(round);
            assert_eq!(buffer.as_ref().len(), 4096);
        }
        assert_eq!(pool.allocations(), 1);
    }

    #[test]
    fn retains_at_most_cap_idle_buffers() {
        let pool = FixedBufferPool::new(64, 2);
        let a = FixedBufferPool::acquire(&pool);
        let b = FixedBufferPool::acquire(&pool);
        let c = FixedBufferPool::acquire(&pool);
        assert_eq!(pool.allocations(), 3);
        drop(a);
        drop(b);
        drop(c);
        // Two recycled, the third released: the first two acquires hit and
        // only the third misses.
        let _first = FixedBufferPool::acquire(&pool);
        let _second = FixedBufferPool::acquire(&pool);
        assert_eq!(pool.allocations(), 3);
        let _third = FixedBufferPool::acquire(&pool);
        assert_eq!(pool.allocations(), 4);
    }

    #[test]
    fn slices_share_bytes_and_recycle_on_last_drop() {
        let pool = FixedBufferPool::new(4096, 1);
        let mut buffer = FixedBufferPool::acquire(&pool);
        buffer
            .as_mut()
            .copy_from_slice(&(0..=255u8).cycle().take(4096).collect::<Vec<_>>());

        let whole = buffer.into_slice();
        let head = whole.slice(0..2048);
        let tail = whole.slice(2048..4096);
        let sub = tail.slice(0..4);
        assert_eq!(&head[..4], &[0, 1, 2, 3]);
        // tail starts at byte 2048, which is 0 modulo 256.
        assert_eq!(&sub[..], &[0, 1, 2, 3]);
        drop(whole);

        // Slice handles dropped but references keep the memory alive: the
        // pool stays empty and a fresh acquire allocates.
        let _next = FixedBufferPool::acquire(&pool);
        assert_eq!(pool.allocations(), 2);

        drop(head);
        drop(tail);
        drop(sub);
        let _reused = FixedBufferPool::acquire(&pool);
        assert_eq!(pool.allocations(), 2, "last reference drop must recycle");
    }

    #[test]
    fn from_vec_does_not_recycle_into_any_pool() {
        let pool = FixedBufferPool::new(16, 2);
        let slice = SlabSlice::from_vec(vec![7u8; 16]);
        assert_eq!(&slice[..], &[7u8; 16]);
        assert_eq!(slice.len(), 16);
        drop(slice);
        let _miss = FixedBufferPool::acquire(&pool);
        assert_eq!(pool.allocations(), 1);
    }

    #[test]
    fn into_slice_len_windows_a_partial_buffer_and_recycles() {
        let pool = FixedBufferPool::new(4096, 1);
        let mut buffer = FixedBufferPool::acquire(&pool);
        buffer.as_mut().fill(0xAB);
        let window = buffer.into_slice_len(1000);
        assert_eq!(window.len(), 1000);
        assert_eq!(&window[..4], &[0xAB; 4]);
        drop(window);
        let _reused = FixedBufferPool::acquire(&pool);
        assert_eq!(
            pool.allocations(),
            1,
            "partially windowed buffer must recycle on last drop"
        );
    }
}
