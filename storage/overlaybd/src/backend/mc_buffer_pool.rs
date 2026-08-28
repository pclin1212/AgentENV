//! Reusable, page-aligned buffers registered with Mooncake's transfer engine.
//!
//! UB and RDMA cannot DMA directly into arbitrary Rust heap allocations.  In
//! particular, the URMA implementation requires a page-aligned memory region.
//! This pool owns aligned allocations, registers each allocation once, and
//! reuses it across reads.  Callers copy the completed read into their normal
//! Rust/ublk destination buffer.

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ffi::{c_int, c_void};
use std::ptr::NonNull;
use std::sync::Mutex;

use anyhow::{bail, Context, Result};

pub type RegisterBufferFn =
    unsafe extern "C" fn(store: *mut c_void, buffer: *mut c_void, size: usize) -> c_int;
pub type UnregisterBufferFn =
    unsafe extern "C" fn(store: *mut c_void, buffer: *mut c_void) -> c_int;

const FALLBACK_PAGE_SIZE: usize = 4096;

fn system_page_size() -> usize {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size > 0 && (page_size as usize).is_power_of_two() {
        page_size as usize
    } else {
        FALLBACK_PAGE_SIZE
    }
}

fn aligned_capacity(size: usize, alignment: usize) -> Result<usize> {
    let remainder = size % alignment;
    if remainder == 0 {
        return Ok(size);
    }
    size.checked_add(alignment - remainder)
        .context("registered Mooncake buffer size overflow")
}

#[derive(Debug)]
struct RegisteredBuffer {
    store: *mut c_void,
    ptr: NonNull<u8>,
    layout: Layout,
    unregister: UnregisterBufferFn,
    registered: bool,
}

// The allocation is exclusively owned by a lease or by the pool's mutex.
unsafe impl Send for RegisteredBuffer {}

impl RegisteredBuffer {
    fn allocate(
        store: *mut c_void,
        size: usize,
        alignment: usize,
        register: RegisterBufferFn,
        unregister: UnregisterBufferFn,
    ) -> Result<Self> {
        let capacity = aligned_capacity(size, alignment)?;
        let layout = Layout::from_size_align(capacity, alignment)
            .context("construct registered Mooncake buffer layout")?;
        let ptr = NonNull::new(unsafe { alloc_zeroed(layout) })
            .context("allocate registered Mooncake buffer")?;

        let mut buffer = Self {
            store,
            ptr,
            layout,
            unregister,
            registered: false,
        };

        // Fault every page in before asking URMA to pin it.  alloc_zeroed may
        // otherwise be backed by lazy anonymous pages on Linux.
        for offset in (0..capacity).step_by(alignment) {
            unsafe { buffer.ptr.as_ptr().add(offset).write_volatile(0) };
        }

        let ret = unsafe { register(store, buffer.as_mut_ptr(), capacity) };
        if ret != 0 {
            bail!(
                "register page-aligned Mooncake buffer ({:p}, {capacity}B) failed: ret={ret}",
                buffer.as_mut_ptr()
            );
        }
        buffer.registered = true;
        Ok(buffer)
    }

    fn capacity(&self) -> usize {
        self.layout.size()
    }

    fn as_mut_ptr(&mut self) -> *mut c_void {
        self.ptr.as_ptr().cast()
    }

    fn as_slice(&self, len: usize) -> &[u8] {
        debug_assert!(len <= self.capacity());
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), len) }
    }
}

impl Drop for RegisteredBuffer {
    fn drop(&mut self) {
        if self.registered {
            unsafe {
                (self.unregister)(self.store, self.ptr.as_ptr().cast());
            }
        }
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

/// Pool of reusable registered buffers for Mooncake UB/RDMA reads.
#[derive(Debug)]
pub struct RegisteredReadBufferPool {
    store: *mut c_void,
    alignment: usize,
    register: RegisterBufferFn,
    unregister: UnregisterBufferFn,
    available: Mutex<Vec<RegisteredBuffer>>,
}

// Mooncake store handles are thread-safe. Buffers are either exclusively held
// by a lease or protected by `available`.
unsafe impl Send for RegisteredReadBufferPool {}
unsafe impl Sync for RegisteredReadBufferPool {}

impl RegisteredReadBufferPool {
    /// Create a pool tied to `store`.
    ///
    /// The pool must be dropped before the Mooncake store handle is destroyed.
    ///
    /// # Safety
    ///
    /// `store` must remain a valid Mooncake handle for the lifetime of this
    /// pool. `register` and `unregister` must be the matching functions for
    /// that handle.
    pub unsafe fn new(
        store: *mut c_void,
        register: RegisterBufferFn,
        unregister: UnregisterBufferFn,
    ) -> Self {
        Self {
            store,
            alignment: system_page_size(),
            register,
            unregister,
            available: Mutex::new(Vec::new()),
        }
    }

    /// Acquire a registered buffer. New page-aligned regions are registered
    /// lazily under peak concurrency and retained for later reads.
    pub fn acquire(&self, size: usize) -> Result<RegisteredReadBufferLease<'_>> {
        if size == 0 {
            bail!("cannot acquire a zero-length registered Mooncake buffer");
        }

        let buffer = {
            let mut available = self
                .available
                .lock()
                .map_err(|_| anyhow::anyhow!("Mooncake registered buffer pool is poisoned"))?;
            let best_fit = available
                .iter()
                .enumerate()
                .filter(|(_, buffer)| buffer.capacity() >= size)
                .min_by_key(|(_, buffer)| buffer.capacity())
                .map(|(index, _)| index);
            best_fit.map(|index| available.swap_remove(index))
        };

        let buffer = match buffer {
            Some(buffer) => buffer,
            None => RegisteredBuffer::allocate(
                self.store,
                size,
                self.alignment,
                self.register,
                self.unregister,
            )?,
        };

        Ok(RegisteredReadBufferLease {
            pool: self,
            buffer: Some(buffer),
        })
    }
}

/// Exclusive lease of a registered buffer. Returning the lease puts the
/// region back into the pool without unregistering it.
pub struct RegisteredReadBufferLease<'a> {
    pool: &'a RegisteredReadBufferPool,
    buffer: Option<RegisteredBuffer>,
}

impl RegisteredReadBufferLease<'_> {
    pub fn as_mut_ptr(&mut self) -> *mut c_void {
        self.buffer
            .as_mut()
            .expect("registered buffer lease must own a buffer")
            .as_mut_ptr()
    }

    pub fn as_slice(&self, len: usize) -> &[u8] {
        self.buffer
            .as_ref()
            .expect("registered buffer lease must own a buffer")
            .as_slice(len)
    }
}

impl Drop for RegisteredReadBufferLease<'_> {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool
                .available
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    static REGISTER_CALLS: AtomicUsize = AtomicUsize::new(0);
    static UNREGISTER_CALLS: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" fn fake_register(
        _store: *mut c_void,
        buffer: *mut c_void,
        size: usize,
    ) -> c_int {
        assert_eq!(buffer as usize % system_page_size(), 0);
        assert_eq!(size % system_page_size(), 0);
        REGISTER_CALLS.fetch_add(1, Ordering::SeqCst);
        0
    }

    unsafe extern "C" fn fake_unregister(_store: *mut c_void, _buffer: *mut c_void) -> c_int {
        UNREGISTER_CALLS.fetch_add(1, Ordering::SeqCst);
        0
    }

    #[test]
    fn aligned_capacity_rounds_to_registration_granularity() {
        assert_eq!(aligned_capacity(1, 4096).unwrap(), 4096);
        assert_eq!(aligned_capacity(4096, 4096).unwrap(), 4096);
        assert_eq!(aligned_capacity(4097, 4096).unwrap(), 8192);
    }

    #[test]
    fn registered_buffers_are_aligned_reused_and_unregistered_on_drop() {
        REGISTER_CALLS.store(0, Ordering::SeqCst);
        UNREGISTER_CALLS.store(0, Ordering::SeqCst);

        let first_ptr;
        {
            let pool = unsafe {
                RegisteredReadBufferPool::new(std::ptr::null_mut(), fake_register, fake_unregister)
            };
            {
                let mut lease = pool.acquire(38).unwrap();
                first_ptr = lease.as_mut_ptr();
                assert_eq!(first_ptr as usize % system_page_size(), 0);
            }
            {
                let mut lease = pool.acquire(38).unwrap();
                assert_eq!(lease.as_mut_ptr(), first_ptr);
            }
            assert_eq!(REGISTER_CALLS.load(Ordering::SeqCst), 1);
            assert_eq!(UNREGISTER_CALLS.load(Ordering::SeqCst), 0);
        }

        assert_eq!(UNREGISTER_CALLS.load(Ordering::SeqCst), 1);
    }
}
