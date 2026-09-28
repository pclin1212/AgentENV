use std::fs::{self, File};
use std::net::{IpAddr, Ipv4Addr};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::{stream::TryStreamExt, StreamExt};
use netlink_packet_route::address::{AddressAttribute, AddressMessage};
use netlink_packet_route::link::{
    InfoData, InfoKind, InfoVeth, LinkAttribute, LinkFlags, LinkInfo,
};
use netlink_packet_route::{AddressFamily, RouteNetlinkMessage};
use nix::mount::{mount, MsFlags};
use nix::sched::{unshare, CloneFlags};
use rtnetlink::packet_core::{
    NetlinkMessage, NetlinkPayload, NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST,
};
use rtnetlink::{new_connection, Handle};
use tracing::{debug, info, warn};

use super::egress_proxy::EgressProxy;
use super::iptables_util::{apply_iptables_commands, IptablesRestoreCommand, OpenFailurePolicy};
use super::policy::{
    initialize_namespace_egress_chain, set_namespace_egress_policy, SandboxNetworkPolicy,
};
use super::{
    mac_string, NetworkAddressPlan, NetworkError, HOST_VETH_PREFIX, MAX_SLOTS, NETNS_PREFIX,
    TAP_MAC,
};
use crate::sandbox::firecracker::{TapHandoff, SANDBOX_TAP_IFACE_NAME};

/// Process-wide baseline network namespace fd.
///
/// Captured once from the current calling thread before any `unshare(CLONE_NEWNET)`.
/// All subsequent slot creations move host-side interfaces back to this namespace.
static HOST_NS_FD: OnceLock<OwnedFd> = OnceLock::new();

const ARP_RETRANS_TIME_MS: &str = "100";
const NEIGH_SYSCTL_RETRIES: usize = 5;
const NEIGH_SYSCTL_RETRY_DELAY_MS: u64 = 20;

/// `_IOW('T', 202, int)`: attach this descriptor to a tun/tap queue.
const TUNSETIFF: libc::c_ulong = 0x4004_54ca;

/// `_IOW('T', 216, int)`: set the vnet header size of this queue.
const TUNSETVNETHDRSZ: libc::c_ulong = 0x4004_54d8;

/// `_IOW('T', 208, c_uint)`: set the offload features of this queue.
const TUNSETOFFLOAD: libc::c_ulong = 0x4004_54d0;

// Queue flags the slot attaches the TAP queue with; they are the contract the
// `fdp:` spec consumer (Firecracker) trusts without validation.
const IFF_TAP: libc::c_short = 0x0002;
const IFF_NO_PI: libc::c_short = 0x1000;
const IFF_VNET_HDR: libc::c_short = 0x4000;

/// The vnet header size Firecracker programs its TAP queues with
/// (`vnet_hdr_len()` in its virtio-net device, the size of its
/// `virtio_net_hdr_v1` binding). Preset at attach so the `fdp:` spec
/// consumer can skip its own TUNSETVNETHDRSZ. Must stay in sync with
/// Firecracker; a mismatch corrupts frames silently.
const FC_VNET_HDR_LEN: libc::c_int = 12;

// TUNSETOFFLOAD feature bits (linux/if_tun.h).
const TUN_F_CSUM: libc::c_uint = 0x01;
const TUN_F_TSO4: libc::c_uint = 0x02;
const TUN_F_TSO6: libc::c_uint = 0x04;
const TUN_F_UFO: libc::c_uint = 0x10;

/// TAP offload flags preset on attach: `TUN_F_CSUM | TUN_F_TSO4 |
/// TUN_F_TSO6 | TUN_F_UFO` — the set Firecracker's `LAUNCHER_PRESET_OFFLOAD`
/// programs for a standard Linux guest's negotiation. Presetting lets the
/// `fdp:` spec consumer skip its own TUNSETOFFLOAD at activate when the
/// guest acked this exact set, taking the last tun ioctl (an rtnl round
/// trip under the global kernel lock) off the snapshot-resume hot path.
/// Must stay in sync with Firecracker; a Firecracker that still programs
/// offload unconditionally just overwrites this with the negotiated set.
const FC_TAP_OFFLOAD_PRESET: libc::c_uint = TUN_F_CSUM | TUN_F_TSO4 | TUN_F_TSO6 | TUN_F_UFO;

// Upper bounds for [`Slot::drain_tap_queue`]: 1024 reads of 64 KiB cap the
// work spent on a queue that keeps receiving frames while being drained.
const TAP_DRAIN_MAX_READS: usize = 1024;
const TAP_DRAIN_BUF_SIZE: usize = 64 * 1024;

/// Get a borrowed reference to the host network namespace fd.
pub(super) fn host_ns_fd() -> BorrowedFd<'static> {
    HOST_NS_FD
        .get_or_init(|| {
            let file = File::open("/proc/thread-self/ns/net")
                .expect("Failed to open host network namespace from /proc/thread-self/ns/net");
            OwnedFd::from(file)
        })
        .as_fd()
}

#[derive(Debug)]
pub(crate) struct Slot {
    pub idx: u32,
    pub namespace_id: String,
    pub host_interaction_ip: Ipv4Addr,
    pub veth_host_ip: Ipv4Addr, // The IP on the Host side interface
    pub veth_vm_ip: Ipv4Addr,   // The IP on the VM/NS side interface (vpeer)
    address_plan: NetworkAddressPlan,
    netns_dir: PathBuf,
    egress_proxy: Arc<EgressProxy>,
    cleanup_armed: bool,
    /// Whether this namespace's user egress chain currently contains rules.
    /// Warm-pool reuse preserves the namespace, so the next tenant may need to
    /// clear rules left by the previous tenant.
    user_egress_rules_present: bool,
    /// Queue descriptor attached to this namespace's persistent tap0 while
    /// `firecracker.preopen_tap` is enabled. The descriptor keeps the queue
    /// (and with it the network namespace) alive across Firecracker process
    /// restarts, so teardown costs no tun ioctls. It is dropped together with
    /// the slot after `cleanup()` has run: cleanup removes the namespace file
    /// and veth, and the namespace itself only goes away once this last
    /// reference closes.
    tap_queue_fd: Option<OwnedFd>,
}

struct NamespaceSetup {
    idx: u32,
    namespace_id: String,
    veth_vm_ip: Ipv4Addr,
    veth_host_ip: Ipv4Addr,
    host_interaction_ip: Ipv4Addr,
    address_plan: NetworkAddressPlan,
    netns_dir: PathBuf,
}

impl Slot {
    fn host_veth_name(idx: u32) -> String {
        format!("{HOST_VETH_PREFIX}{idx}")
    }

    pub(super) fn new(
        idx: u32,
        address_plan: NetworkAddressPlan,
        netns_dir: PathBuf,
        egress_proxy: Arc<EgressProxy>,
    ) -> Result<Self, NetworkError> {
        // Validation for zero and overflow.
        if idx == 0 || idx >= (MAX_SLOTS as u32) {
            return Err(NetworkError::SlotOutOfRange {
                idx,
                max: (MAX_SLOTS as u32) - 1,
            });
        }

        let namespace_id = format!("{}{}", NETNS_PREFIX, uuid::Uuid::now_v7());
        let (host_interaction_ip, veth_host_ip, veth_vm_ip) = address_plan
            .slot_ips(idx)
            .map_err(NetworkError::NamespaceError)?;

        Ok(Self {
            idx,
            namespace_id,
            host_interaction_ip,
            veth_host_ip,
            veth_vm_ip,
            address_plan,
            netns_dir,
            egress_proxy,
            cleanup_armed: false,
            user_egress_rules_present: false,
            tap_queue_fd: None,
        })
    }

    /// Creates the network infrastructure for this slot using a separate thread
    /// to isolate namespace operations.
    #[tracing::instrument(
        skip(self),
        fields(
            slot = self.idx,
            namespace_id = %self.namespace_id,
            host_veth = %Self::host_veth_name(self.idx),
            host_interaction_ip = %self.host_interaction_ip
        )
    )]
    pub(super) fn create_network(&mut self) -> Result<(), NetworkError> {
        // Arm drop cleanup as soon as we begin touching kernel networking state.
        // If setup fails midway, Drop can still perform best-effort cleanup.
        self.cleanup_armed = true;

        // Capture individual fields rather than cloning `self`. Slot is not Clone
        // intentionally — a clone would carry Drop semantics and tear down the live
        // network when the thread finishes.
        let setup = NamespaceSetup {
            idx: self.idx,
            namespace_id: self.namespace_id.clone(),
            veth_vm_ip: self.veth_vm_ip,
            veth_host_ip: self.veth_host_ip,
            host_interaction_ip: self.host_interaction_ip,
            address_plan: self.address_plan,
            netns_dir: self.netns_dir.clone(),
        };
        let idx = setup.idx;
        let veth_host_ip = setup.veth_host_ip;
        let veth_vm_ip = setup.veth_vm_ip;
        let host_interaction_ip = setup.host_interaction_ip;

        // Get the global host NS FD to move the interface back later.
        // This uses /proc/1/ns/net to ensure we always get the true host namespace,
        // even when called from threads that may have modified their namespaces.
        let host_ns_fd = host_ns_fd();

        // Spawn a thread to perform namespace operations safely.
        let handle = thread::spawn(move || Self::setup_namespace_internal(setup, host_ns_fd));

        let tap_queue_fd = match handle.join() {
            Ok(result) => result.map_err(NetworkError::NamespaceError),
            Err(e) => Err(NetworkError::NamespaceError(anyhow!(
                "Network setup thread panicked: {:?}",
                e
            ))),
        }?;
        self.tap_queue_fd = tap_queue_fd;

        // Configure the Host side now.
        Self::run_async(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("Failed to build tokio runtime")?;
            rt.block_on(Self::configure_host_interface_async(
                idx,
                veth_host_ip,
                veth_vm_ip,
                host_interaction_ip,
            ))
        })
        .map_err(NetworkError::NamespaceError)?;

        // Reduce ARP retransmit delay on host-side veth to avoid resume tail latency (issue #272).
        let veth_name = Self::host_veth_name(idx);
        Self::tune_neigh_retrans_time_ms(&veth_name);

        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        fields(
            slot = setup.idx,
            namespace_id = %setup.namespace_id,
            veth_vm_ip = %setup.veth_vm_ip,
            veth_host_ip = %setup.veth_host_ip,
            host_interaction_ip = %setup.host_interaction_ip
        )
    )]
    fn setup_namespace_internal(
        setup: NamespaceSetup,
        host_ns_fd: BorrowedFd<'static>,
    ) -> Result<Option<OwnedFd>> {
        let NamespaceSetup {
            idx,
            namespace_id,
            veth_vm_ip,
            veth_host_ip,
            host_interaction_ip,
            address_plan,
            netns_dir,
        } = setup;

        // 1. Create/Open Target Network Namespace
        if !netns_dir.exists() {
            fs::create_dir_all(&netns_dir).with_context(|| {
                format!(
                    "Failed to create AENV network namespace directory {}",
                    netns_dir.display()
                )
            })?;
        }
        let netns_path = netns_dir.join(&namespace_id);
        if !netns_path.exists() {
            File::create(&netns_path).context("Failed to create netns file")?;
        }

        // Unshare logic
        unshare(CloneFlags::CLONE_NEWNET).context("Failed to unshare(CLONE_NEWNET)")?;

        // Bind mount the new namespace to make it persistent/named
        mount(
            Some("/proc/thread-self/ns/net"),
            &netns_path,
            None::<&str>,
            MsFlags::MS_BIND,
            None::<&str>,
        )
        .context("Failed to bind mount new namespace")?;

        // Configure interfaces inside the namespace via netlink
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to build tokio runtime in thread")?;
        let tap_ip = address_plan.tap_ip();
        let vm_link_prefix = address_plan.vm_link_prefix();

        rt.block_on(Self::configure_namespace_interfaces(
            idx,
            veth_vm_ip,
            veth_host_ip,
            tap_ip,
            vm_link_prefix,
            host_ns_fd,
        ))?;

        // Enable IP forwarding inside this namespace so packets received on tap0
        // (from the VM) can be forwarded to vpeer (towards the host/internet).
        fs::write("/proc/sys/net/ipv4/ip_forward", "1")
            .context("Failed to enable IP forwarding in namespace")?;

        // Reduce ARP retransmit delay for faster resume (issue #272)
        Self::tune_neigh_retrans_time_ms("tap0");
        Self::tune_neigh_retrans_time_ms("vpeer");

        // IPTables Setup
        Self::configure_namespace_iptables_rules(
            host_interaction_ip,
            veth_vm_ip,
            address_plan.vm_ip(),
            &address_plan.internal_egress_denied_cidrs(),
        )?;

        // Attach the slot-owned TAP queue last, still inside this namespace:
        // TUNSETIFF resolves the interface by name, so it must run where the
        // interface lives. Once attached, the queue is usable from any thread.
        let tap_queue_fd = if crate::cfg::ConfigManager::global_config()
            .firecracker
            .preopen_tap
        {
            Some(Self::attach_tap_queue(SANDBOX_TAP_IFACE_NAME)?)
        } else {
            None
        };

        Ok(tap_queue_fd)
    }

    /// Attaches a queue to the named persistent TAP interface and returns the
    /// descriptor. Must run inside the network namespace that owns the
    /// interface. The queue is attached with the flags, vnet header size, and
    /// offload features Firecracker trusts under the `fdp:` spec, and
    /// attaching matches the interface owner rather than requiring
    /// capabilities.
    pub(crate) fn attach_tap_queue(tap_name: &str) -> Result<OwnedFd> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            // O_CLOEXEC keeps the descriptor out of unrelated children exec'd
            // by this process; spawns handing it to Firecracker dup it
            // explicitly. O_NONBLOCK lets the queue be drained cheaply.
            .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open("/dev/net/tun")
            .context("open /dev/net/tun")?;
        let bytes = tap_name.as_bytes();
        anyhow::ensure!(
            !bytes.is_empty() && bytes.len() < libc::IFNAMSIZ,
            "tap interface name {tap_name:?} must be 1 to {} bytes",
            libc::IFNAMSIZ - 1
        );
        let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                ifr.ifr_name.as_mut_ptr().cast(),
                bytes.len(),
            );
            ifr.ifr_ifru.ifru_flags = IFF_TAP | IFF_NO_PI | IFF_VNET_HDR;
        }
        // SAFETY: `file` is an open tun descriptor and `ifr` a valid request.
        if (unsafe { libc::ioctl(file.as_raw_fd(), TUNSETIFF, &mut ifr) }) < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("attach TAP queue to {tap_name}"));
        }
        // SAFETY: `file` is an open tun descriptor. TUNSETVNETHDRSZ is
        // `_IOW('T', 216, int)`: the kernel reads the size through a
        // userspace pointer (get_user), so pass a reference, not the
        // value itself — matching Firecracker's `ioctl_with_ref` call.
        if (unsafe { libc::ioctl(file.as_raw_fd(), TUNSETVNETHDRSZ, &FC_VNET_HDR_LEN) }) < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("preset vnet header size on TAP queue {tap_name}"));
        }
        // SAFETY: `file` is an open tun descriptor. TUNSETOFFLOAD is
        // `_IOW('T', 208, c_uint)` but the kernel consumes the flags from
        // the ioctl argument value itself (`set_offload(tun, arg)`), so
        // pass the value, not a reference — matching Firecracker's
        // `ioctl_with_val` call.
        if (unsafe { libc::ioctl(file.as_raw_fd(), TUNSETOFFLOAD, FC_TAP_OFFLOAD_PRESET) }) < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("preset offload on TAP queue {tap_name}"));
        }
        Ok(file.into())
    }

    /// The slot-owned TAP handoff for the next Firecracker spawn: `Some`
    /// exactly when this slot holds an attached queue, which
    /// `create_network` sets up if and only if `firecracker.preopen_tap` is
    /// enabled. This is the single decision point shared by the spawn-side
    /// fd handoff and the `fdp:` `host_dev_name` spec — never decide
    /// the two separately. The returned handoff borrows the queue, so the
    /// descriptor is guaranteed to stay open for the spawn call consuming
    /// it.
    pub(crate) fn tap_handoff(&self) -> Option<TapHandoff<'_>> {
        self.tap_queue_fd.as_ref().map(TapHandoff::new)
    }

    /// Discards frames queued while the slot was live or pooled so the next
    /// tenant never reads the previous lifecycle's traffic. All descriptors
    /// of the queue share one socket, so draining through this slot's
    /// descriptor also clears what a parked warm Firecracker would later feed
    /// to a restored guest. Best-effort: errors are logged and ignored.
    pub(crate) fn drain_tap_queue(&mut self) {
        let Some(queue) = self.tap_queue_fd.as_ref() else {
            return;
        };
        let fd = queue.as_raw_fd();
        let mut scratch = [0u8; TAP_DRAIN_BUF_SIZE];
        for _ in 0..TAP_DRAIN_MAX_READS {
            // SAFETY: `fd` is an open descriptor and `scratch` bounds the
            // write.
            let read_bytes = unsafe { libc::read(fd, scratch.as_mut_ptr().cast(), scratch.len()) };
            if read_bytes > 0 {
                continue;
            }
            // The descriptor is O_NONBLOCK, so an EAGAIN failure simply means
            // the queue is empty; only other errors are worth logging.
            if read_bytes < 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::EAGAIN) {
                    debug!(
                        slot = self.idx,
                        %error,
                        "failed to read while draining tap queue"
                    );
                }
            }
            return;
        }
        debug!(
            slot = self.idx,
            "tap queue still readable after drain limit; leaving remaining frames"
        );
    }

    /// Configures all network interfaces inside the namespace:
    /// creates veth pair, moves host end back, sets up loopback/vpeer/tap0, and adds default route.
    #[tracing::instrument(
        skip(host_ns_fd),
        fields(
            slot = idx,
            veth_vm_ip = %veth_vm_ip,
            veth_host_ip = %veth_host_ip
        )
    )]
    async fn configure_namespace_interfaces(
        idx: u32,
        veth_vm_ip: Ipv4Addr,
        veth_host_ip: Ipv4Addr,
        tap_ip: Ipv4Addr,
        vm_link_prefix: u8,
        host_ns_fd: BorrowedFd<'_>,
    ) -> Result<()> {
        let (connection, handle, _) = new_connection().context("Failed to connect to netlink")?;
        tokio::spawn(connection);

        // Create Veth Pair (aenv-{idx} and vpeer)
        let veth_name = Self::host_veth_name(idx);
        let vpeer_name = "vpeer";

        // Create veth pair using netlink
        let mut veth_msg = netlink_packet_route::link::LinkMessage::default();
        veth_msg
            .attributes
            .push(LinkAttribute::IfName(veth_name.clone()));

        let mut peer_msg = netlink_packet_route::link::LinkMessage::default();
        peer_msg
            .attributes
            .push(LinkAttribute::IfName(vpeer_name.into()));

        let info = LinkAttribute::LinkInfo(vec![
            LinkInfo::Kind(InfoKind::Veth),
            LinkInfo::Data(InfoData::Veth(InfoVeth::Peer(peer_msg))),
        ]);
        veth_msg.attributes.push(info);

        handle
            .link()
            .add(veth_msg)
            .execute()
            .await
            .context("Failed to create veth pair")?;

        // Move veth (host end) back to Host NS
        let mut links = handle.link().get().match_name(veth_name.clone()).execute();
        if let Some(link) = links.try_next().await? {
            let mut msg = netlink_packet_route::link::LinkMessage::default();
            msg.header.index = link.header.index;
            msg.attributes
                .push(LinkAttribute::NetNsFd(host_ns_fd.as_raw_fd()));

            handle
                .link()
                .set(msg)
                .execute()
                .await
                .context("Failed to move veth to host ns")?;
        } else {
            return Err(anyhow!("Created veth interface not found"));
        }

        // Configure IPs inside NS
        // Loopback UP
        let mut lo_links = handle.link().get().match_name("lo".to_string()).execute();
        if let Some(lo) = lo_links.try_next().await? {
            let mut msg = netlink_packet_route::link::LinkMessage::default();
            msg.header.index = lo.header.index;
            msg.header.flags.insert(LinkFlags::Up);
            msg.header.change_mask.insert(LinkFlags::Up);

            handle
                .link()
                .set(msg)
                .execute()
                .await
                .context("Failed to set lo up")?;
        }

        // Vpeer setup
        // For /31 point-to-point links (RFC 3021), we should NOT set a broadcast address.
        let vpeer_name_str = "vpeer";
        let mut vpeer_links = handle
            .link()
            .get()
            .match_name(vpeer_name_str.to_string())
            .execute();
        if let Some(vpeer) = vpeer_links.try_next().await? {
            // Add IP without broadcast (RFC 3021 for /31)
            Self::add_address_no_broadcast(&handle, vpeer.header.index, veth_vm_ip, 31)
                .await
                .context("Failed to add address to vpeer")?;

            // Set vpeer UP
            let mut link_msg = netlink_packet_route::link::LinkMessage::default();
            link_msg.header.index = vpeer.header.index;
            link_msg.header.flags.insert(LinkFlags::Up);
            link_msg.header.change_mask.insert(LinkFlags::Up);
            handle
                .link()
                .set(link_msg)
                .execute()
                .await
                .context("Failed to set vpeer up")?;
        }

        // Create tap0 interface (Tun/Tap)
        let status = crate::privileges::run_with_scoped_capabilities(
            &[crate::privileges::CAP_NET_ADMIN],
            || {
                Command::new("ip")
                    .args(["tuntap", "add", "tap0", "mode", "tap"])
                    .status()
                    .context("Failed to execute ip tuntap")
            },
        )?;
        if !status.success() {
            return Err(anyhow!("ip tuntap add failed"));
        }

        // Pin tap0's link address to the plan constant while the device is
        // still down: a restored guest's neighbour table is frozen with the
        // `tap_ip -> tap0 MAC` mapping of whichever slot captured its
        // snapshot, so every slot must present the same tap0 MAC or replies
        // from a guest restored onto a foreign slot would be dropped here.
        let tap_mac = mac_string(&TAP_MAC);
        let status = crate::privileges::run_with_scoped_capabilities(
            &[crate::privileges::CAP_NET_ADMIN],
            || {
                Command::new("ip")
                    .args(["link", "set", "dev", "tap0", "address", &tap_mac])
                    .status()
                    .context("Failed to execute ip link set tap0 address")
            },
        )?;
        if !status.success() {
            return Err(anyhow!("ip link set tap0 address failed"));
        }

        // Enable tap0 and add strict IP via Netlink
        let mut tap_links = handle.link().get().match_name("tap0".to_string()).execute();
        if let Some(tap) = tap_links.try_next().await? {
            handle
                .address()
                .add(tap.header.index, IpAddr::V4(tap_ip), vm_link_prefix)
                .execute()
                .await
                .context("Failed to add address to tap0")?;

            let mut link_msg = netlink_packet_route::link::LinkMessage::default();
            link_msg.header.index = tap.header.index;
            link_msg.header.flags.insert(LinkFlags::Up);
            link_msg.header.change_mask.insert(LinkFlags::Up);
            handle
                .link()
                .set(link_msg)
                .execute()
                .await
                .context("Failed to set tap0 up")?;
        }

        // Add default route via veth_host_ip (the host side of the veth pair)
        // Using rtnetlink's RouteMessageBuilder API
        let route_msg = rtnetlink::RouteMessageBuilder::<std::net::Ipv4Addr>::new()
            .gateway(veth_host_ip)
            .build();
        handle
            .route()
            .add(route_msg)
            .execute()
            .await
            .context("Failed to add default route")?;

        Ok(())
    }

    /// Builds the kernel `ip=` boot argument for this slot's VM network configuration.
    ///
    /// Format: `ip=<vm_ip>::<tap_ip>:<netmask>:<hostname>:<iface>:<autoconf>:<dns>`
    pub(crate) fn build_ip_boot_arg(&self) -> String {
        let dns_ip = self.guest_dns_server();
        format!(
            "ip={}::{}:{}:instance:eth0:off:{}",
            self.address_plan.vm_ip(),
            self.address_plan.tap_ip(),
            self.address_plan.vm_link_mask(),
            dns_ip
        )
    }

    pub(crate) fn guest_dns_server(&self) -> Ipv4Addr {
        resolve_guest_dns_server()
    }

    pub(crate) fn namespace_path(&self) -> std::path::PathBuf {
        self.netns_dir.join(&self.namespace_id)
    }

    pub(crate) fn set_egress_policy(
        &mut self,
        policy: Option<&SandboxNetworkPolicy>,
    ) -> Result<()> {
        // Policy updates run through the owning FirecrackerSandbox's mutable
        // operation lock; cleanup owns this Slot exclusively. Avoid holding a
        // second lock across namespace I/O, iptables, or proxy joins.
        let wants_rules = policy.is_some_and(SandboxNetworkPolicy::has_runtime_egress_rules);
        if !wants_rules && !self.user_egress_rules_present {
            return Ok(());
        }

        let requires_egress_proxy = policy.is_some_and(SandboxNetworkPolicy::requires_egress_proxy);
        let had_active_proxy_policy = self.egress_proxy.has_active(self.host_interaction_ip);
        if requires_egress_proxy {
            self.egress_proxy
                .ensure_listener(self.host_interaction_ip, &self.namespace_path())?;
            self.egress_proxy.prepare(
                self.host_interaction_ip,
                policy.expect("proxy policy must be present when interception is requested"),
            );
            // There was no old proxy policy to preserve. Activating before the
            // redirect is installed keeps the old default-allow behavior while
            // the namespace rules are being committed.
            if !had_active_proxy_policy {
                self.egress_proxy.activate(self.host_interaction_ip);
            }
        }

        let netns_path = self.namespace_path();
        let egress_proxy_port = self.egress_proxy.port();
        let policy = policy.cloned();
        let handle = thread::spawn(move || -> Result<()> {
            let netns = File::open(&netns_path).with_context(|| {
                format!("failed to open network namespace {}", netns_path.display())
            })?;
            nix::sched::setns(netns.as_fd(), CloneFlags::CLONE_NEWNET)
                .context("failed to enter sandbox network namespace")?;
            set_namespace_egress_policy(policy.as_ref(), egress_proxy_port)
        });

        let result = match handle.join() {
            Ok(result) => result,
            Err(e) => Err(anyhow!("egress policy setup thread panicked: {:?}", e)),
        };
        if result.is_ok() {
            self.user_egress_rules_present = wants_rules;
            if requires_egress_proxy && had_active_proxy_policy {
                self.egress_proxy.activate(self.host_interaction_ip);
            } else if !requires_egress_proxy {
                self.egress_proxy.deactivate(self.host_interaction_ip);
            }
        } else if requires_egress_proxy {
            if had_active_proxy_policy {
                self.egress_proxy.discard_pending(self.host_interaction_ip);
            } else {
                self.egress_proxy.teardown(self.host_interaction_ip);
            }
        }
        result
    }

    /// Configures iptables rules inside the namespace for VM traffic routing.
    /// This includes:
    /// - Enabling IP forwarding so the namespace can route between tap0 and vpeer.
    /// - FORWARD rules to permit traffic between the VM (tap0) and the host veth (vpeer).
    /// - SNAT/DNAT for host<->VM communication via host_interaction_ip.
    #[tracing::instrument(fields(vm_ip = %vm_ip, host_interaction_ip = %host_interaction_ip))]
    fn configure_namespace_iptables_rules(
        host_interaction_ip: Ipv4Addr,
        veth_vm_ip: Ipv4Addr,
        vm_ip: Ipv4Addr,
        internal_egress_denied_cidrs: &[String],
    ) -> Result<()> {
        let commands = [
            // FORWARD: Allow traffic from VM (tap0) to host/internet (vpeer).
            IptablesRestoreCommand::Append {
                table: "filter",
                chain: "FORWARD",
                rule: "-i tap0 -o vpeer -j ACCEPT".to_string(),
            },
            // FORWARD: Allow established/related traffic from host/internet (vpeer) back to VM (tap0).
            IptablesRestoreCommand::Append {
                table: "filter",
                chain: "FORWARD",
                rule: "-i vpeer -o tap0 -m state --state RELATED,ESTABLISHED -j ACCEPT".to_string(),
            },
            // SNAT: Rewrite source IP from the VM to the slot's host interaction IP.
            // This covers both host<->VM communication and internet-bound traffic from the VM.
            // The host then applies its own MASQUERADE to reach the internet.
            IptablesRestoreCommand::Append {
                table: "nat",
                chain: "POSTROUTING",
                rule: format!("-o vpeer -s {} -j SNAT --to {}", vm_ip, host_interaction_ip),
            },
            // Namespace-local egress proxy connections originate from vpeer's
            // address rather than the guest address above. Give them the same
            // routable slot identity so host FORWARD/MASQUERADE rules apply.
            IptablesRestoreCommand::Append {
                table: "nat",
                chain: "POSTROUTING",
                rule: format!("-o vpeer -s {veth_vm_ip} -j SNAT --to {host_interaction_ip}"),
            },
            // DNAT: Rewrite destination IP from the host interaction IP to the VM.
            // This allows the host to reach the VM using the unique HostIP.
            IptablesRestoreCommand::Append {
                table: "nat",
                chain: "PREROUTING",
                rule: format!("-i vpeer -d {} -j DNAT --to {}", host_interaction_ip, vm_ip),
            },
        ];

        apply_iptables_commands(&commands, OpenFailurePolicy::ReturnErr)?;
        initialize_namespace_egress_chain(resolve_guest_dns_server(), internal_egress_denied_cidrs)
    }

    fn tune_neigh_retrans_time_ms(interface: &str) {
        let retrans_path = format!("/proc/sys/net/ipv4/neigh/{interface}/retrans_time_ms");

        for attempt in 0..=NEIGH_SYSCTL_RETRIES {
            match fs::write(&retrans_path, ARP_RETRANS_TIME_MS) {
                Ok(()) => {
                    return;
                }
                Err(err)
                    if err.kind() == std::io::ErrorKind::NotFound
                        && attempt < NEIGH_SYSCTL_RETRIES =>
                {
                    debug!(
                        interface,
                        path = %retrans_path,
                        attempt = attempt + 1,
                        error = %err,
                        "ARP retransmit sysctl not ready; retrying"
                    );
                    thread::sleep(Duration::from_millis(NEIGH_SYSCTL_RETRY_DELAY_MS));
                }
                Err(err) => {
                    warn!(
                        interface,
                        path = %retrans_path,
                        error = %err,
                        "failed to configure ARP retransmit delay"
                    );
                    return;
                }
            }
        }
    }

    /// Helper to run async code, handling the case where we might already be in a tokio runtime.
    fn run_async<F, T>(f: F) -> Result<T>
    where
        F: FnOnce() -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let has_current_runtime =
            std::panic::catch_unwind(|| tokio::runtime::Handle::try_current().is_ok())
                .unwrap_or(false);

        if has_current_runtime {
            // We're inside a runtime, spawn a blocking thread to avoid nested runtime
            std::thread::spawn(f)
                .join()
                .map_err(|e| anyhow!("Thread panicked: {:?}", e))?
        } else {
            // Not inside a runtime, we can run directly
            f()
        }
    }

    #[tracing::instrument(
        fields(
            slot = idx,
            host_veth = %Self::host_veth_name(idx),
            veth_host_ip = %veth_host_ip,
            veth_vm_ip = %veth_vm_ip,
            host_interaction_ip = %host_interaction_ip
        )
    )]
    async fn configure_host_interface_async(
        idx: u32,
        veth_host_ip: Ipv4Addr,
        veth_vm_ip: Ipv4Addr,
        host_interaction_ip: Ipv4Addr,
    ) -> Result<()> {
        let (connection, handle, _) = new_connection().context("Netlink connect host")?;
        tokio::spawn(connection);

        let veth_name = Self::host_veth_name(idx);

        // Wait/Check for interface
        let mut links = handle.link().get().match_name(veth_name.clone()).execute();
        if let Some(link) = links.try_next().await? {
            // Add only the veth link IP, not the host_interaction_ip.
            // host_interaction_ip is used as a routing destination, not an interface address
            //
            // For /31 point-to-point links (RFC 3021), we should NOT set a broadcast address.
            Self::add_address_no_broadcast(&handle, link.header.index, veth_host_ip, 31)
                .await
                .context("Failed to add IP to host veth")?;

            // Set UP
            let mut link_msg = netlink_packet_route::link::LinkMessage::default();
            link_msg.header.index = link.header.index;
            link_msg.header.flags.insert(LinkFlags::Up);
            link_msg.header.change_mask.insert(LinkFlags::Up);
            handle
                .link()
                .set(link_msg)
                .execute()
                .await
                .context("Failed to set host veth up")?;

            // Add route from host to namespace: packets destined for host_interaction_ip
            // are forwarded through vpeer IP (in the namespace) as the gateway.
            // This is how the host can reach the VM - the namespace's DNAT rule then
            // translates the destination to the VM's internal IP.
            //
            // Using `ip route add` command as netlink route add with gateway on /31
            // subnet has compatibility issues with some kernel configurations.
            let output = crate::privileges::run_with_scoped_capabilities(
                &[crate::privileges::CAP_NET_ADMIN],
                || {
                    std::process::Command::new("ip")
                        .args([
                            "route",
                            "add",
                            &format!("{}/32", host_interaction_ip),
                            "via",
                            &veth_vm_ip.to_string(),
                            "dev",
                            &veth_name,
                        ])
                        .output()
                        .context("Failed to execute ip route add")
                },
            )?;

            if !output.status.success() {
                return Err(anyhow!(
                    "Failed to add route to {}/32 via {} dev {}: {}",
                    host_interaction_ip,
                    veth_vm_ip,
                    veth_name,
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        } else {
            return Err(anyhow!(
                "Host veth interface {} not found after move",
                veth_name
            ));
        }
        Ok(())
    }

    /// Async helper to delete veth interface.
    /// Idempotent: succeeds even if the interface doesn't exist.
    async fn delete_veth_interface_async(idx: u32) -> Result<()> {
        let (connection, handle, _) = new_connection()?;
        tokio::spawn(connection);

        let veth_name = Self::host_veth_name(idx);
        let mut links = handle.link().get().match_name(veth_name.clone()).execute();

        // try_next returns Err if interface doesn't exist (ENODEV), treat as success
        match links.try_next().await {
            Ok(Some(link)) => {
                // Interface exists, try to delete; ignore "not found" race
                if let Err(e) = handle.link().del(link.header.index).execute().await {
                    let msg = e.to_string();
                    if !msg.contains("No such device") && !msg.contains("ENODEV") {
                        return Err(e.into());
                    }
                }
            }
            Ok(None) => {} // Interface not found
            Err(e) => {
                let msg = e.to_string();
                if !msg.contains("No such device") && !msg.contains("ENODEV") {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    /// Deletes host veth using Tokio-assisted netlink cleanup.
    ///
    /// This is the regular (non-shutdown) cleanup path and preserves the
    /// previous runtime behavior used by normal slot release.
    fn delete_host_veth_interface_with_tokio(idx: u32) -> Result<()> {
        Self::run_async(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| anyhow!("Failed to build runtime: {}", e))?;
            rt.block_on(Self::delete_veth_interface_async(idx))
        })
    }

    /// Deletes host veth using `ip link del`.
    ///
    /// Kept synchronous as the fallback path for shutdown/exit cleanup where
    /// Tokio context may already be unavailable.
    fn delete_host_veth_interface_sync(idx: u32) -> Result<()> {
        let veth_name = Self::host_veth_name(idx);
        let output = crate::privileges::run_with_scoped_capabilities(
            &[crate::privileges::CAP_NET_ADMIN],
            || {
                Command::new("ip")
                    .args(["link", "del", &veth_name])
                    .output()
                    .context("Failed to execute ip link del")
            },
        )?;

        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr_lower = stderr.to_lowercase();
        if stderr_lower.contains("cannot find device")
            || stderr_lower.contains("no such device")
            || stderr_lower.contains("not found")
        {
            return Ok(());
        }

        Err(anyhow!(
            "Failed to delete veth interface {}: {}",
            veth_name,
            stderr.trim()
        ))
    }

    /// Tries Tokio-assisted veth cleanup first and falls back to synchronous
    /// cleanup on either regular error or panic.
    #[tracing::instrument(fields(slot = idx, host_veth = %Self::host_veth_name(idx)))]
    fn delete_host_veth_interface(idx: u32) -> Result<()> {
        match std::panic::catch_unwind(|| Self::delete_host_veth_interface_with_tokio(idx)) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                info!(
                    slot = idx,
                    error = %err,
                    "tokio-assisted slot cleanup failed; falling back to sync cleanup"
                );
                Self::delete_host_veth_interface_sync(idx)
            }
            Err(_) => {
                info!(
                    slot = idx,
                    "tokio-assisted slot cleanup panicked; falling back to sync cleanup"
                );
                Self::delete_host_veth_interface_sync(idx)
            }
        }
    }

    /// Cleans up the network resources for this slot.
    /// This includes deleting the host-side veth interface, removing the network namespace,
    /// and removing the host-side MASQUERADE rule.
    /// Idempotent: safe to call multiple times or concurrently.
    #[tracing::instrument(
        skip(self),
        fields(
            slot = self.idx,
            namespace_id = %self.namespace_id,
            host_veth = %Self::host_veth_name(self.idx),
            host_interaction_ip = %self.host_interaction_ip,
            force_sync
        )
    )]
    pub(super) fn cleanup(&mut self, force_sync: bool) -> Result<(), NetworkError> {
        // Skip cleanup for slots that never attempted network setup.
        // This avoids touching host networking state for logical-only Slot values.
        if !self.cleanup_armed {
            return Ok(());
        }
        self.cleanup_armed = false;

        // A namespace-local listener pins the namespace. Stop proxy acceptance
        // before removing the veth and unmounting the namespace, including
        // panic/drop cleanup paths that bypass the normal release path.
        self.egress_proxy.teardown(self.host_interaction_ip);

        // 1. Delete Host Veth Interface (this destroys the pair)
        let delete_result = if force_sync {
            Self::delete_host_veth_interface_sync(self.idx)
        } else {
            Self::delete_host_veth_interface(self.idx)
        };
        if let Err(e) = delete_result {
            self.cleanup_armed = true;
            return Err(NetworkError::NamespaceError(e));
        }

        // 2. Unmount Netns Bind Mount (may need multiple unmounts if mounted multiple times)
        let netns_path = self.namespace_path();
        let path = netns_path.as_path();
        if path.exists() {
            loop {
                match nix::mount::umount(path) {
                    Ok(_) => continue,
                    Err(nix::errno::Errno::EINVAL) => break, // Not mounted anymore
                    Err(nix::errno::Errno::ENOENT) => break, // File removed by another process
                    Err(e) => {
                        self.cleanup_armed = true;
                        return Err(NetworkError::NamespaceError(anyhow!(
                            "Failed to unmount netns: {}",
                            e
                        )));
                    }
                }
            }

            // 3. Delete Netns File (ignore NotFound - another process may have deleted it)
            match fs::remove_file(path) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    self.cleanup_armed = true;
                    return Err(NetworkError::IoError(e));
                }
            }
        }

        Ok(())
    }

    /// Add an IPv4 address to an interface without setting broadcast address.
    /// This is needed for /31 point-to-point links (RFC 3021) where there is no
    /// broadcast address. The rtnetlink crate's AddressMessageBuilder incorrectly
    /// calculates a broadcast address for /31 networks.
    async fn add_address_no_broadcast(
        handle: &Handle,
        if_index: u32,
        addr: Ipv4Addr,
        prefix_len: u8,
    ) -> Result<()> {
        let mut msg = AddressMessage::default();
        msg.header.family = AddressFamily::Inet;
        msg.header.prefix_len = prefix_len;
        msg.header.index = if_index;

        // Add Address and Local attributes (required for IPv4)
        // Do NOT add Broadcast attribute for /31 networks
        msg.attributes
            .push(AddressAttribute::Address(IpAddr::V4(addr)));
        msg.attributes
            .push(AddressAttribute::Local(IpAddr::V4(addr)));

        let mut req = NetlinkMessage::from(RouteNetlinkMessage::NewAddress(msg));
        req.header.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL;

        let mut response = handle.clone().request(req)?;
        while let Some(message) = response.next().await {
            if let NetlinkPayload::Error(err) = message.payload {
                return Err(anyhow!("Netlink error: {:?}", err));
            }
        }
        Ok(())
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup(true) {
            warn!(slot = self.idx, error = %e, "slot drop cleanup failed");
        }
    }
}

fn resolve_guest_dns_server() -> Ipv4Addr {
    for path in ["/run/systemd/resolve/resolv.conf", "/etc/resolv.conf"] {
        if let Ok(contents) = fs::read_to_string(path) {
            if let Some(ip) = parse_nameserver_ipv4(&contents) {
                return ip;
            }
        }
    }

    let fallback = Ipv4Addr::new(8, 8, 8, 8);
    warn!(dns = %fallback, "falling back to public DNS for guest network");
    fallback
}

fn parse_nameserver_ipv4(contents: &str) -> Option<Ipv4Addr> {
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let mut parts = line.split_whitespace();
        let Some(directive) = parts.next() else {
            continue;
        };
        if directive != "nameserver" {
            continue;
        }

        let Some(candidate) = parts.next() else {
            continue;
        };
        let ip = match candidate.parse::<Ipv4Addr>() {
            Ok(ip) => ip,
            Err(_) => continue,
        };

        if ip.is_loopback() || ip.is_unspecified() {
            continue;
        }

        return Some(ip);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::network::GUEST_MAC;

    /// `_IOR('T', 215, int)`: read back a queue's vnet header size.
    const TUNGETVNETHDRSZ: libc::c_ulong = 0x8004_54d7;

    fn test_slot(idx: u32, address_plan: NetworkAddressPlan) -> Result<Slot, NetworkError> {
        Slot::new(
            idx,
            address_plan,
            std::env::temp_dir().join("aenv-network-tests/netns"),
            EgressProxy::new(),
        )
    }

    fn command_stdout(command: &str, args: &[&str]) -> Option<String> {
        let output = Command::new(command).args(args).output().ok()?;
        if !output.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn host_veth_exists(slot_idx: u32) -> bool {
        let veth_name = Slot::host_veth_name(slot_idx);
        command_stdout("ip", &["-o", "link", "show", &veth_name])
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
    }

    /// Runs `ip <args>` inside the slot's namespace via nsenter. Tests only:
    /// production code enters namespaces with dedicated threads.
    fn nsenter_ip(netns_path: &std::path::Path, args: &[&str]) -> Option<String> {
        let output = Command::new("nsenter")
            .arg(format!("--net={}", netns_path.display()))
            .arg("ip")
            .args(args)
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn unused_test_slot() -> Slot {
        let address_plan = NetworkAddressPlan::default();
        (30_000..MAX_SLOTS as u32)
            .find(|idx| !host_veth_exists(*idx))
            .and_then(|idx| test_slot(idx, address_plan).ok())
            .expect("failed to find an unused high-numbered network test slot")
    }

    #[test]
    fn test_slot_ip_calculation() {
        let address_plan = NetworkAddressPlan::default();

        // Test Slot 1
        let slot1 = test_slot(1, address_plan).expect("Slot 1 should be valid");
        assert_eq!(slot1.idx, 1);
        assert_eq!(slot1.host_interaction_ip.to_string(), "10.11.0.1");

        // Base 10.12.0.0. Offset 1*2 = 2.
        // Host: .2, VM: .3
        assert_eq!(slot1.veth_host_ip.to_string(), "10.12.0.2");
        assert_eq!(slot1.veth_vm_ip.to_string(), "10.12.0.3");

        // Test Slot 2
        let slot2 = test_slot(2, address_plan).expect("Slot 2 should be valid");
        assert_eq!(slot2.host_interaction_ip.to_string(), "10.11.0.2");
        // Offset 2*2 = 4. Host .4, VM .5
        assert_eq!(slot2.veth_host_ip.to_string(), "10.12.0.4");
        assert_eq!(slot2.veth_vm_ip.to_string(), "10.12.0.5");
    }

    #[test]
    fn custom_address_plan_calculates_slot_ips_and_boot_arg() {
        let config = crate::cfg::NetworkConfig {
            egress: crate::cfg::NetworkEgressConfig::default(),
            internal: crate::cfg::NetworkInternalConfig {
                host_interaction_cidr: "100.64.0.0/16".to_string(),
                veth_cidr: "100.65.0.0/16".to_string(),
            },
        };
        let address_plan = NetworkAddressPlan::from_config(&config).unwrap();
        let slot = test_slot(2, address_plan).expect("slot should be valid");

        assert_eq!(slot.host_interaction_ip.to_string(), "100.64.0.2");
        assert_eq!(slot.veth_host_ip.to_string(), "100.65.0.4");
        assert_eq!(slot.veth_vm_ip.to_string(), "100.65.0.5");
        assert!(slot
            .build_ip_boot_arg()
            .starts_with("ip=169.254.0.21::169.254.0.22:255.255.255.252:"));
    }

    #[test]
    fn test_slot_overflow() {
        let address_plan = NetworkAddressPlan::default();

        // Max valid index is 32767
        let max_valid = 32767;
        let slot = test_slot(max_valid, address_plan);
        assert!(slot.is_ok());

        // 32768 should fail
        let overflow = test_slot(32768, address_plan);
        assert!(overflow.is_err());
        match overflow {
            Err(NetworkError::SlotOutOfRange { idx, max }) => {
                assert_eq!(idx, 32768);
                assert_eq!(max, 32767);
            }
            _ => panic!("Expected SlotOutOfRange error"),
        }
    }

    #[test]
    fn parse_nameserver_ipv4_prefers_non_loopback_ipv4() {
        let conf = r#"
            # generated by systemd-resolved
            nameserver 127.0.0.53
            nameserver 10.0.0.2
            nameserver 8.8.8.8
        "#;
        assert_eq!(
            parse_nameserver_ipv4(conf),
            Some(Ipv4Addr::new(10, 0, 0, 2))
        );
    }

    #[test]
    fn parse_nameserver_ipv4_ignores_non_ipv4_entries() {
        let conf = r#"
            nameserver ::1
            nameserver not_an_ip
            search example.com
        "#;
        assert_eq!(parse_nameserver_ipv4(conf), None);
    }

    #[test]
    fn parse_nameserver_ipv4_accepts_link_local_dns() {
        let conf = "nameserver 169.254.169.253\n";
        assert_eq!(
            parse_nameserver_ipv4(conf),
            Some(Ipv4Addr::new(169, 254, 169, 253))
        );
    }

    #[test]
    fn parse_nameserver_ipv4_skips_malformed_nameserver_lines() {
        let conf = r#"
            nameserver
            nameserver 10.1.2.1
        "#;
        assert_eq!(
            parse_nameserver_ipv4(conf),
            Some(Ipv4Addr::new(10, 1, 2, 1))
        );
    }

    #[test]
    fn empty_egress_policy_skips_known_clean_slot() {
        let mut slot = test_slot(1, NetworkAddressPlan::default()).unwrap();
        let empty_policy = SandboxNetworkPolicy::default();

        slot.set_egress_policy(None).unwrap();
        slot.set_egress_policy(Some(&empty_policy)).unwrap();

        assert!(!slot.user_egress_rules_present);
    }

    #[test]
    fn failed_egress_cleanup_keeps_slot_marked_dirty() {
        let mut slot = test_slot(1, NetworkAddressPlan::default()).unwrap();
        slot.user_egress_rules_present = true;

        assert!(slot.set_egress_policy(None).is_err());

        assert!(slot.user_egress_rules_present);
    }

    #[test]
    fn failed_egress_apply_keeps_clean_slot_marked_clean() {
        let mut slot = test_slot(1, NetworkAddressPlan::default()).unwrap();
        let policy = SandboxNetworkPolicy::new(
            true,
            crate::sandbox::network::BaseSandboxNetworkPolicy::Deny,
            crate::sandbox::network::SandboxNetworkEgressPolicy::default(),
        );

        assert!(slot.set_egress_policy(Some(&policy)).is_err());

        assert!(!slot.user_egress_rules_present);
    }

    #[test]
    fn drain_tap_queue_is_noop_without_attached_queue() {
        let mut slot = test_slot(1, NetworkAddressPlan::default()).unwrap();
        // create_network never ran, so the slot holds no queue descriptor.
        assert!(slot.tap_queue_fd.is_none());
        slot.drain_tap_queue();
    }

    #[test]
    fn planned_macs_are_locally_administered_unicast_and_distinct() {
        for mac in [GUEST_MAC, TAP_MAC] {
            assert_eq!(
                mac[0] & 0b0000_0011,
                0b0000_0010,
                "plan MACs must be locally administered unicast addresses"
            );
        }
        assert_ne!(GUEST_MAC, TAP_MAC, "guest and tap0 must not share a MAC");
        assert_eq!(mac_string(&GUEST_MAC), "02:61:65:6e:76:21");
    }

    #[test]
    fn tap_handoff_requires_attached_queue() {
        let slot = test_slot(1, NetworkAddressPlan::default()).unwrap();
        // No queue was attached (create_network never ran), so even with
        // `preopen_tap` enabled there is nothing to hand off.
        assert!(slot.tap_handoff().is_none());
    }

    #[test]
    #[ignore = "requires CAP_NET_ADMIN/CAP_SYS_ADMIN and affects system configuration"]
    fn test_network_lifecycle() {
        crate::logging::init_for_tests();

        // Use a free high slot ID to avoid collisions with dev/prod and stale
        // devices from interrupted test runs.
        let mut slot = unused_test_slot();

        // 1. Create Network
        // This requires CAP_NET_ADMIN and CAP_SYS_ADMIN.
        match slot.create_network() {
            Ok(_) => {}
            Err(e) => {
                // If it fails due to permissions, we skip, otherwise fail
                let err_str = e.to_string();
                if err_str.contains("Operation not permitted") || err_str.contains("EPERM") {
                    println!("Skipping test due to lack of permissions");
                    return;
                }
                panic!("Failed to create network: {:?}", e);
            }
        }

        // 1b. The slot owns an attached TAP queue exactly when creation
        // succeeded and preopen_tap is enabled.
        if crate::cfg::ConfigManager::global_config()
            .firecracker
            .preopen_tap
        {
            assert!(
                slot.tap_queue_fd.is_some(),
                "slot must hold the attached TAP queue descriptor"
            );
            // The `fdp:` spec consumer trusts this preset; read it back.
            let fd = slot.tap_queue_fd.as_ref().unwrap().as_raw_fd();
            let mut hdr_len: libc::c_int = 0;
            // SAFETY: `fd` is an attached tun queue descriptor and
            // `hdr_len` a valid out-pointer.
            let ret = unsafe { libc::ioctl(fd, TUNGETVNETHDRSZ, &mut hdr_len) };
            assert_eq!(
                ret,
                0,
                "TUNGETVNETHDRSZ failed: {:?}",
                std::io::Error::last_os_error()
            );
            assert_eq!(hdr_len, FC_VNET_HDR_LEN, "vnet header size must be preset");
            slot.drain_tap_queue();
        } else {
            assert!(slot.tap_queue_fd.is_none());
        }

        // 1c. tap0 presents the pinned plan MAC, so a guest restored onto
        // this slot whose snapshot froze `tap_ip -> tap0 MAC` on another
        // slot still addresses us correctly.
        let netns_path = slot.namespace_path();
        let tap_link = nsenter_ip(&netns_path, &["-o", "link", "show", "dev", "tap0"])
            .expect("nsenter ip link show tap0");
        let planned_tap_mac = mac_string(&TAP_MAC);
        assert!(
            tap_link.contains(&planned_tap_mac),
            "tap0 must present the pinned plan MAC {planned_tap_mac}: {tap_link}"
        );

        // 2. Verify Namespace File
        let netns_path = slot.namespace_path();
        assert!(
            netns_path.exists(),
            "Namespace file should exist after creation"
        );

        // 3. Verify Host Veth Interface
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let slot_idx = slot.idx;
        rt.block_on(async {
            let (connection, handle, _) = new_connection().unwrap();
            tokio::spawn(connection);
            let mut links = handle
                .link()
                .get()
                .match_name(Slot::host_veth_name(slot_idx))
                .execute();
            let link = links.try_next().await.unwrap();
            assert!(
                link.is_some(),
                "{} should exist on host",
                Slot::host_veth_name(slot_idx)
            );
        });

        // 4. Cleanup
        let clean_res = slot.cleanup(false);
        assert!(clean_res.is_ok(), "cleanup should succeed");

        // 5. Verify Removal
        assert!(!netns_path.exists(), "Namespace file should be removed");
        rt.block_on(async {
            let (connection, handle, _) = new_connection().unwrap();
            tokio::spawn(connection);
            let mut links = handle
                .link()
                .get()
                .match_name(Slot::host_veth_name(slot_idx))
                .execute();
            // ENODEV (-19) is returned when interface doesn't exist, which is expected
            let link = links.try_next().await.unwrap_or(None);
            assert!(
                link.is_none(),
                "{} should be gone",
                Slot::host_veth_name(slot_idx)
            );
        });
    }
}
