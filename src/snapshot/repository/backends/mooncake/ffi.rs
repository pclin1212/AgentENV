//! Direct `extern "C"` declarations matching `mooncake-store/include/store_c.h`.
//!
//! All C API functions follow the convention that `char *` parameters are
//! consumed by the call — the caller may free them after the call returns.

use std::ffi::{c_char, c_int, c_void};

pub type MoonCakeStore = *mut c_void;

/// Mirrors `mooncake_replicate_config_t` from store_c.h exactly.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct MoonCakeReplicateConfig {
    pub replica_num: usize,
    pub with_soft_pin: c_int,
    pub with_hard_pin: c_int,
    pub preferred_segments: *const *const c_char,
    pub preferred_segments_count: usize,
}

#[link(name = "mooncake_store")]
extern "C" {
    // ── Lifecycle ────────────────────────────────────────────────────────

    pub fn mooncake_store_create() -> MoonCakeStore;
    pub fn mooncake_store_destroy(store: MoonCakeStore);
    pub fn mooncake_store_setup(
        store: MoonCakeStore,
        local_hostname: *const c_char,
        metadata_server: *const c_char,
        global_segment_size: u64,
        local_buffer_size: u64,
        protocol: *const c_char,
        device_name: *const c_char,
        master_server_addr: *const c_char,
    ) -> c_int;
    // ── Put ──────────────────────────────────────────────────────────────

    pub fn mooncake_store_put(
        store: MoonCakeStore,
        key: *const c_char,
        value: *const c_void,
        size: usize,
        config: *const MoonCakeReplicateConfig,
    ) -> c_int;

    // ── Get (returns bytes read, or -1 on error) ─────────────────────────

    pub fn mooncake_store_get_into(
        store: MoonCakeStore,
        key: *const c_char,
        buffer: *mut c_void,
        size: usize,
    ) -> i64;

    // ── Existence / Size ─────────────────────────────────────────────────

    pub fn mooncake_store_is_exist(store: MoonCakeStore, key: *const c_char) -> c_int;
    pub fn mooncake_store_get_size(store: MoonCakeStore, key: *const c_char) -> i64;
    // ── Remove ───────────────────────────────────────────────────────────

    pub fn mooncake_store_remove(store: MoonCakeStore, key: *const c_char, force: c_int) -> c_int;
    pub fn mooncake_store_remove_by_regex(
        store: MoonCakeStore,
        pattern: *const c_char,
        force: c_int,
    ) -> i64;
}
