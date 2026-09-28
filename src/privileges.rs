use std::ffi::{CStr, CString};
use std::fs::File;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, AsRawFd, OwnedFd, RawFd};
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::ptr;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use rustix::event::{poll, PollFd, PollFlags, Timespec};
use rustix::process::{pidfd_open, pidfd_send_signal, Pid, PidfdFlags, Signal};
use tokio::io::unix::AsyncFd;
use tracing::warn;

pub use linux_cap::{CAP_NET_ADMIN, CAP_SYS_ADMIN};

fn capability_name(capability: i32) -> &'static str {
    match capability {
        CAP_NET_ADMIN => "CAP_NET_ADMIN",
        CAP_SYS_ADMIN => "CAP_SYS_ADMIN",
        _ => "unknown capability",
    }
}

/// Validate the capability contract required by the non-root runtime.
///
/// The inheritable and permitted checks are intentional: privileged helper
/// processes receive only their required capability through the ambient set
/// immediately before `exec`.
pub fn require_runtime_capabilities() -> Result<()> {
    let sets = linux_cap::CapabilitySets::current().context("read process capability sets")?;
    let is_root = nix::unistd::Uid::effective().is_root();
    let missing = [CAP_NET_ADMIN, CAP_SYS_ADMIN]
        .into_iter()
        .filter(|capability| {
            if is_root {
                !sets.effective(*capability).unwrap_or(false)
            } else {
                !sets.is_delegable(*capability).unwrap_or(false)
            }
        })
        .map(capability_name)
        .collect::<Vec<_>>();

    if !missing.is_empty() {
        bail!(
            "AENV runtime is missing required Linux capabilities: {}. Start it with CAP_NET_ADMIN and CAP_SYS_ADMIN in the inheritable, permitted, and effective sets (the installed systemd unit configures this automatically)",
            missing.join(", ")
        );
    }
    Ok(())
}

/// Clear ambient capabilities from the server so ordinary executables do not
/// inherit its network and namespace privileges.
pub fn clear_ambient_capabilities() -> Result<()> {
    linux_cap::clear_ambient_capabilities().context("clear ambient Linux capabilities")
}

/// Run an operation on a short-lived thread with an exact capability set.
///
/// Linux capabilities and network namespace membership are thread-scoped. The
/// new thread inherits the caller's namespace, receives only `capabilities`,
/// and exits after the operation. Commands spawned by the operation inherit the
/// requested capabilities through the ambient set without requiring a
/// `pre_exec` hook in the multi-threaded server.
pub fn run_with_scoped_capabilities<T, F>(capabilities: &[i32], operation: F) -> Result<T>
where
    T: Send,
    F: FnOnce() -> Result<T> + Send,
{
    thread::scope(|scope| -> Result<T> {
        let launcher = thread::Builder::new()
            .name("aenv-capability-command".to_string())
            .spawn_scoped(scope, move || {
                linux_cap::configure_current_process_capabilities(capabilities)
                    .context("scope command capabilities")?;
                operation()
            })
            .context("spawn capability-scoped command thread")?;

        launcher
            .join()
            .map_err(|panic| anyhow!("capability-scoped command thread panicked: {panic:?}"))?
    })
}

/// Everything needed to exec a scoped child process.
///
/// The child always inherits the server's environment (`environ`), and stdin is
/// always `/dev/null`.
pub struct ScopedSpawnSpec {
    /// `argv` including `argv[0]`. `argv[0]` doubles as the executable path
    /// and must be absolute: `posix_spawn` does not search `PATH`.
    pub argv: Vec<CString>,
    /// Working directory applied inside the child before `exec`.
    pub cwd: Option<CString>,
    /// Child stdout log file; `None` routes stdout to `/dev/null`.
    pub stdout: Option<File>,
    /// Child stderr log file; `None` routes stderr to `/dev/null`.
    pub stderr: Option<File>,
    /// Start the child in its own process group (`setpgid(0, 0)`).
    pub process_group: bool,
    /// Network namespace entered on the launcher thread before the spawn, so
    /// the child inherits it. Capabilities are scoped after entering.
    pub netns: Option<OwnedFd>,
    /// Child-private descriptor handoff applied as a `dup2` file action before
    /// `exec` (the network slot's TAP queue parked on descriptor 3 for
    /// Firecracker). Because the action runs inside the spawned child, the
    /// server's descriptor table is never touched and concurrent spawns cannot
    /// observe each other's source descriptors. `dup2` clears `FD_CLOEXEC` on
    /// the target unless it is a same-number no-op, so the source must differ
    /// from the target.
    pub fd_handoff: Option<(RawFd, RawFd)>,
    /// Capabilities the launcher thread keeps while spawning; the child
    /// inherits exactly this ambient set. An empty slice drops every
    /// capability before the child is created.
    pub capabilities: &'static [i32],
}

/// Spawn a child from a short-lived launcher thread with an exact capability
/// set, entering an optional network namespace first, and exec it with any
/// descriptor handoffs expressed as posix_spawn file actions.
///
/// glibc implements `posix_spawn` as `clone(CLONE_VM | CLONE_VFORK)` plus
/// `execve`: the child shares the server's address space until it execs, so a
/// large server pays no page-table copy here — unlike the `fork`+`exec`
/// fallback that a `pre_exec` hook forces. That is why all child-side setup
/// rides on [`ScopedSpawnSpec`] file actions and attributes instead of a hook.
pub async fn spawn_scoped(spec: ScopedSpawnSpec) -> io::Result<ScopedChild> {
    let (sender, receiver) = tokio::sync::oneshot::channel();

    thread::Builder::new()
        .name("aenv-process-launcher".to_string())
        .spawn(move || {
            let result = (|| {
                if let Some(netns) = &spec.netns {
                    // SAFETY: `netns` is an open network namespace descriptor and
                    // this short-lived launcher thread has not spawned a child yet.
                    let rc = unsafe { libc::setns(netns.as_raw_fd(), libc::CLONE_NEWNET) };
                    if rc != 0 {
                        return Err(io::Error::last_os_error());
                    }
                }
                linux_cap::configure_current_process_capabilities(spec.capabilities)?;
                posix_spawn_exec(&spec)
            })();

            if let Err(Ok(child)) = sender.send(result) {
                // The receiver is gone; the handle's drop kills and reaps.
                drop(child);
            }
        })?;

    receiver
        .await
        .map_err(|_| io::Error::other("scoped command launcher exited without returning a child"))?
}

/// A child process spawned by [`spawn_scoped`].
///
/// The handle owns a pidfd for the child and is the child's only reaper.
/// Waiting rides on the pidfd's epoll readiness, so no global signal state
/// is involved and exit notifications cannot coalesce or be swallowed by
/// another consumer. Dropping it SIGKILLs the child through the pidfd
/// (which pins the task, so a recycled pid can never be signaled) and
/// reaps it with a bounded wait.
///
/// `pidfd_open` requires Linux 5.3+, which the ublk-based storage path
/// already exceeds.
pub struct ScopedChild {
    pid: u32,
    pidfd: OwnedFd,
    status: Option<ExitStatus>,
}

impl ScopedChild {
    /// The child stays a zombie until this handle reaps it (nothing sets
    /// SIGCHLD to `SIG_IGN`, which would auto-reap), so the pid cannot be
    /// recycled between the spawn and this call; a pidfd for an
    /// already-exited child is simply readable right away.
    fn new(pid: u32) -> io::Result<Self> {
        let raw_pid = Pid::from_raw(pid as i32)
            .ok_or_else(|| io::Error::other("spawned pid does not fit pid_t"))?;
        let pidfd = pidfd_open(raw_pid, PidfdFlags::empty())?;
        Ok(Self {
            pid,
            pidfd,
            status: None,
        })
    }

    /// The child's pid. Stays readable after exit (until reaped), so callers
    /// can report it; signaling after reaping is refused by [`Self::start_kill`].
    pub fn id(&self) -> u32 {
        self.pid
    }

    /// Returns the exit status if the child has exited, reaping it.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(status));
        }
        if let Some(raw) = try_reap_status(self.pid)? {
            let status = ExitStatus::from_raw(raw);
            self.status = Some(status);
            return Ok(Some(status));
        }
        Ok(None)
    }

    /// Waits for the child to exit and returns its exit status, reaping it.
    ///
    /// The pidfd becomes readable when the child exits; waiting on it
    /// through the reactor needs no global signal state. A spurious
    /// wakeup just re-polls, never errors.
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.status {
            return Ok(status);
        }
        let pidfd = AsyncFd::new(self.pidfd.try_clone()?)?;
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(status);
            }
            let mut guard = pidfd.readable().await?;
            if let Some(status) = self.try_wait()? {
                return Ok(status);
            }
            guard.clear_ready();
        }
    }

    /// SIGKILLs the child. A no-op once the child has been reaped.
    pub fn start_kill(&mut self) -> io::Result<()> {
        if self.status.is_some() {
            return Ok(());
        }
        // Signal through the pidfd: it pins the task, so there is no
        // pid-reuse window to reason about, unlike signaling by pid.
        pidfd_send_signal(self.pidfd.as_fd(), Signal::KILL).map_err(io::Error::from)
    }
}

impl Drop for ScopedChild {
    fn drop(&mut self) {
        if self.status.is_some() {
            return;
        }
        if let Err(err) = self.start_kill() {
            warn!(
                pid = self.pid,
                error = %err,
                "failed to kill scoped child process on drop"
            );
        }
        // Bounded reap so the killed child does not linger as a zombie.
        // SIGKILL is unignorable, so the child normally exits within
        // milliseconds; a single bounded wait also works on threads without
        // a Tokio runtime (the pools' process-exit hooks), and a leftover
        // zombie there is reparented to init when the server exits.
        let mut pollfds = [PollFd::new(&self.pidfd, PollFlags::IN)];
        let timeout =
            Timespec::try_from(Duration::from_millis(100)).expect("100ms always fits a timespec");
        let _ = poll(&mut pollfds, Some(&timeout));
        match try_reap_status(self.pid) {
            Ok(Some(_)) => {}
            Ok(None) => warn!(pid = self.pid, "timed out reaping scoped child process"),
            Err(err) => {
                warn!(
                    pid = self.pid,
                    error = %err,
                    "failed to reap scoped child process"
                );
            }
        }
    }
}

// The libc crate does not expose `environ` on Linux; glibc guarantees the
// symbol. Passing it makes the child inherit the current environment.
extern "C" {
    static mut environ: *mut libc::c_char;
}

/// Execs `spec` on the calling thread and returns the child handle.
fn posix_spawn_exec(spec: &ScopedSpawnSpec) -> io::Result<ScopedChild> {
    let program = spec
        .argv
        .first()
        .ok_or_else(|| io::Error::other("scoped spawn requires argv[0] to be the program path"))?;

    let mut actions = FileActions::new()?;
    // stdin is always /dev/null; stdout/stderr are the log files or /dev/null.
    actions.add_open_devnull(libc::STDIN_FILENO)?;
    match &spec.stdout {
        Some(file) => actions.add_dup2(file.as_raw_fd(), libc::STDOUT_FILENO)?,
        None => actions.add_open_devnull(libc::STDOUT_FILENO)?,
    }
    match &spec.stderr {
        Some(file) => actions.add_dup2(file.as_raw_fd(), libc::STDERR_FILENO)?,
        None => actions.add_open_devnull(libc::STDERR_FILENO)?,
    }
    if let Some((source, target)) = spec.fd_handoff {
        actions.add_dup2(source, target)?;
    }
    if let Some(cwd) = &spec.cwd {
        actions.add_chdir(cwd)?;
    }

    let mut attr = SpawnAttr::new()?;
    let mut flags = 0;
    if spec.process_group {
        spawn_result(unsafe { libc::posix_spawnattr_setpgroup(&mut attr.inner, 0) })?;
        flags |= libc::POSIX_SPAWN_SETPGROUP;
    }
    // Caught signals reset to their default disposition and SIGPIPE is
    // restored to SIG_DFL; the signal mask inherits.
    attr.reset_sigpipe_default()?;
    flags |= libc::POSIX_SPAWN_SETSIGDEF;
    spawn_result(unsafe {
        libc::posix_spawnattr_setflags(&mut attr.inner, flags as libc::c_short)
    })?;

    let mut argv: Vec<*mut libc::c_char> = spec
        .argv
        .iter()
        .map(|arg| arg.as_ptr().cast_mut())
        .collect();
    argv.push(ptr::null_mut());

    let mut pid: libc::pid_t = 0;
    // SAFETY: `program`, `argv`, and the attribute/action guards are valid for
    // the duration of the call.
    spawn_result(unsafe {
        libc::posix_spawn(
            &mut pid,
            program.as_ptr(),
            &actions.inner,
            &attr.inner,
            argv.as_ptr(),
            ptr::addr_of!(environ),
        )
    })?;
    ScopedChild::new(pid as u32)
}

/// Maps a spawn function's return value: glibc's spawn family returns an error
/// number directly instead of setting `errno` (mirrors how std maps
/// `posix_spawn` failures).
fn spawn_result(rc: libc::c_int) -> io::Result<()> {
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(rc))
    }
}

/// Non-blocking reap of a spawned pid. `Some` carries the raw wait status; the
/// pid stays reserved until this returns, so callers may still signal pids
/// that return `None`.
fn try_reap_status(pid: u32) -> io::Result<Option<libc::c_int>> {
    let mut status: libc::c_int = 0;
    loop {
        // SAFETY: plain waitpid on this handle's own child.
        let rc = unsafe { libc::waitpid(pid as libc::pid_t, &mut status, libc::WNOHANG) };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        return Ok((rc == pid as libc::pid_t).then_some(status));
    }
}

/// posix_spawn file actions with cleanup on every path.
struct FileActions {
    inner: libc::posix_spawn_file_actions_t,
}

impl FileActions {
    fn new() -> io::Result<Self> {
        let mut inner = MaybeUninit::uninit();
        spawn_result(unsafe { libc::posix_spawn_file_actions_init(inner.as_mut_ptr()) })?;
        // SAFETY: `posix_spawn_file_actions_init` just initialized the value.
        Ok(Self {
            inner: unsafe { inner.assume_init() },
        })
    }

    fn add_dup2(&mut self, source: RawFd, target: RawFd) -> io::Result<()> {
        spawn_result(unsafe {
            libc::posix_spawn_file_actions_adddup2(&mut self.inner, source, target)
        })
    }

    fn add_open_devnull(&mut self, target: RawFd) -> io::Result<()> {
        const DEVNULL: &CStr = c"/dev/null";
        spawn_result(unsafe {
            libc::posix_spawn_file_actions_addopen(
                &mut self.inner,
                target,
                DEVNULL.as_ptr(),
                libc::O_RDONLY,
                0,
            )
        })
    }

    fn add_chdir(&mut self, cwd: &CStr) -> io::Result<()> {
        // Available since glibc 2.29, which is the server's floor.
        spawn_result(unsafe {
            libc::posix_spawn_file_actions_addchdir_np(&mut self.inner, cwd.as_ptr())
        })
    }
}

impl Drop for FileActions {
    fn drop(&mut self) {
        // SAFETY: the value was initialized in `new` and is not used after.
        unsafe { libc::posix_spawn_file_actions_destroy(&mut self.inner) };
    }
}

/// posix_spawn attributes with cleanup on every path.
struct SpawnAttr {
    inner: libc::posix_spawnattr_t,
}

impl SpawnAttr {
    fn new() -> io::Result<Self> {
        let mut inner = MaybeUninit::uninit();
        spawn_result(unsafe { libc::posix_spawnattr_init(inner.as_mut_ptr()) })?;
        // SAFETY: `posix_spawnattr_init` just initialized the value.
        Ok(Self {
            inner: unsafe { inner.assume_init() },
        })
    }

    fn reset_sigpipe_default(&mut self) -> io::Result<()> {
        let mut defaults = MaybeUninit::<libc::sigset_t>::uninit();
        // SAFETY: populating a stack-allocated sigset.
        unsafe {
            if libc::sigemptyset(defaults.as_mut_ptr()) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::sigaddset(defaults.as_mut_ptr(), libc::SIGPIPE) != 0 {
                return Err(io::Error::last_os_error());
            }
            spawn_result(libc::posix_spawnattr_setsigdefault(
                &mut self.inner,
                defaults.as_ptr(),
            ))
        }
    }
}

impl Drop for SpawnAttr {
    fn drop(&mut self) {
        // SAFETY: the value was initialized in `new` and is not used after.
        unsafe { libc::posix_spawnattr_destroy(&mut self.inner) };
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::fd::AsRawFd;

    use anyhow::ensure;
    use nix::sched::CloneFlags;
    use tempfile::tempdir;

    use super::*;

    const CAPABILITY_STATUS_FIELDS: [&str; 4] = ["CapInh:", "CapPrm:", "CapEff:", "CapAmb:"];

    fn status_field<'a>(status: &'a str, field: &str) -> &'a str {
        status
            .lines()
            .find_map(|line| line.strip_prefix(field))
            .unwrap_or_else(|| panic!("{field} is missing from process status"))
            .trim()
    }

    fn current_capability_status() -> Result<Vec<String>> {
        let status = fs::read_to_string("/proc/thread-self/status")?;
        Ok(CAPABILITY_STATUS_FIELDS
            .iter()
            .map(|field| status_field(&status, field).to_string())
            .collect())
    }

    /// Status fields whose values prove the scoped-spawn capability contract.
    ///
    /// On `execve` the kernel regrants an euid-0 child the full bounding set
    /// in `CapPrm`/`CapEff` unless `SECBIT_NOROOT` is set, so under root those
    /// two fields say nothing about what the scoped spawn delegated. The
    /// inheritable set survives `execve` unchanged and the ambient set can
    /// only survive within the permitted ∩ inheritable intersection, so
    /// `CapInh`/`CapAmb` stay zero (or exactly the requested capability)
    /// regardless of the runner's uid.
    fn capability_assertion_fields() -> &'static [&'static str] {
        if nix::unistd::Uid::effective().is_root() {
            &["CapInh:", "CapAmb:"]
        } else {
            &CAPABILITY_STATUS_FIELDS
        }
    }

    fn assert_child_has_no_capabilities(status: &str) {
        for field in capability_assertion_fields() {
            assert_eq!(
                status_field(status, field),
                "0000000000000000",
                "child retained {field}"
            );
        }
    }

    fn sh_spec(script: &str) -> ScopedSpawnSpec {
        ScopedSpawnSpec {
            argv: vec![
                CString::from(c"/bin/sh"),
                CString::from(c"-c"),
                CString::new(script).expect("script has no NUL"),
            ],
            cwd: None,
            stdout: None,
            stderr: None,
            process_group: false,
            netns: None,
            fd_handoff: None,
            capabilities: &[],
        }
    }

    /// Runs `sh -c script` with stdout captured to a temp file and waits for
    /// the child to exit.
    async fn sh_with_spec(
        script: &str,
        configure: impl FnOnce(&mut ScopedSpawnSpec),
    ) -> Result<(ExitStatus, String)> {
        let temp = tempdir()?;
        let stdout_path = temp.path().join("stdout");
        let mut spec = sh_spec(script);
        spec.stdout = Some(fs::File::create(&stdout_path)?);
        configure(&mut spec);

        let mut child = spawn_scoped(spec).await?;
        let status = child.wait().await?;
        let output = fs::read_to_string(&stdout_path)?;
        drop(temp);
        Ok((status, output))
    }

    #[test]
    fn parses_capability_sets() {
        let status =
            "CapInh:\t0000000000201000\nCapPrm:\t0000000000201000\nCapEff:\t0000000000201000\n";
        let sets = linux_cap::CapabilitySets::from_proc_status(status).unwrap();
        assert!(sets.is_delegable(CAP_NET_ADMIN).unwrap());
        assert!(sets.is_delegable(CAP_SYS_ADMIN).unwrap());
    }

    #[test]
    fn capability_must_be_present_in_all_required_sets() {
        let status =
            "CapInh:\t0000000000201000\nCapPrm:\t0000000000201000\nCapEff:\t0000000000001000\n";
        let sets = linux_cap::CapabilitySets::from_proc_status(status).unwrap();
        assert!(sets.is_delegable(CAP_NET_ADMIN).unwrap());
        assert!(!sets.is_delegable(CAP_SYS_ADMIN).unwrap());
    }

    #[tokio::test]
    async fn scoped_spawn_waits_for_exit_status() -> Result<()> {
        let (status, _) = sh_with_spec("exit 3", |_| {}).await?;

        assert_eq!(status.code(), Some(3));
        Ok(())
    }

    #[tokio::test]
    async fn scoped_spawn_hands_fd_to_child_via_file_action() -> Result<()> {
        let temp = tempdir()?;
        let marker = temp.path().join("marker");
        fs::write(&marker, b"marker")?;
        // std opens with O_CLOEXEC: the descriptor surviving into the execed
        // child proves the dup2 file action cleared FD_CLOEXEC on its target,
        // the same contract the TAP queue handoff relies on.
        let marker_file = fs::File::open(&marker)?;

        let (status, output) = sh_with_spec("readlink /proc/self/fd/3", |spec| {
            spec.fd_handoff = Some((marker_file.as_raw_fd(), 3));
        })
        .await?;

        assert!(status.success());
        assert_eq!(output.trim(), marker.to_string_lossy());
        Ok(())
    }

    #[tokio::test]
    async fn scoped_spawn_clears_child_capabilities_without_mutating_caller() -> Result<()> {
        let caller_capabilities = current_capability_status()?;

        let (status, stdout) = sh_with_spec("cat /proc/thread-self/status", |_| {}).await?;

        assert!(status.success());
        assert_child_has_no_capabilities(&stdout);
        assert_eq!(current_capability_status()?, caller_capabilities);
        Ok(())
    }

    #[tokio::test]
    async fn scoped_child_drop_kills_and_reaps_child() -> Result<()> {
        let temp = tempdir()?;
        let marker = temp.path().join("child-started");
        let spec = sh_spec(&format!("touch '{}' && exec sleep 30", marker.display()));
        let child = spawn_scoped(spec).await?;
        let pid = child.id();

        for _ in 0..100 {
            if marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(marker.exists(), "child did not start");

        drop(child);

        for _ in 0..100 {
            if !std::path::Path::new(&format!("/proc/{pid}")).exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires CAP_NET_ADMIN to delegate a capability to the child"]
    async fn scoped_spawn_delegates_only_the_requested_capability() -> Result<()> {
        ensure!(
            linux_cap::has_effective_capabilities(&[CAP_NET_ADMIN])?,
            "test requires CAP_NET_ADMIN"
        );
        let caller_capabilities = current_capability_status()?;

        let (status, stdout) = sh_with_spec("cat /proc/thread-self/status", |spec| {
            spec.capabilities = &[CAP_NET_ADMIN];
        })
        .await?;

        assert!(status.success());
        // Under root, execve regrants CapPrm/CapEff from the bounding set;
        // CapInh/CapAmb keep exactly the delegated capability either way.
        for field in capability_assertion_fields() {
            assert_eq!(
                status_field(&stdout, field),
                "0000000000001000",
                "child received an unexpected {field} value"
            );
        }
        assert_eq!(current_capability_status()?, caller_capabilities);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires CAP_SYS_ADMIN to create and enter a network namespace"]
    async fn scoped_spawn_enters_netns_and_drops_capabilities() -> Result<()> {
        let caller_namespace = fs::read_link("/proc/thread-self/ns/net")?;
        let caller_capabilities = current_capability_status()?;
        let netns: fs::File = thread::spawn(|| -> Result<fs::File> {
            nix::sched::unshare(CloneFlags::CLONE_NEWNET)?;
            Ok(fs::File::open("/proc/thread-self/ns/net")?)
        })
        .join()
        .map_err(|panic| anyhow::anyhow!("network namespace thread panicked: {panic:?}"))??;
        let target_namespace = fs::read_link(format!("/proc/self/fd/{}", netns.as_raw_fd()))?;
        let netns_fd = OwnedFd::from(netns);

        let (status, stdout) = sh_with_spec(
            "readlink /proc/thread-self/ns/net; cat /proc/thread-self/status",
            |spec| spec.netns = Some(netns_fd),
        )
        .await?;
        let (child_namespace, child_status) = stdout
            .split_once('\n')
            .context("child output is missing process status")?;

        assert!(status.success());
        assert_eq!(child_namespace, target_namespace.to_string_lossy());
        assert_child_has_no_capabilities(child_status);
        assert_eq!(fs::read_link("/proc/thread-self/ns/net")?, caller_namespace);
        assert_eq!(current_capability_status()?, caller_capabilities);
        Ok(())
    }
}
