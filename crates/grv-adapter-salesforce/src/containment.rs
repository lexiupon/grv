//! The helper trampoline installs inherited containment in a fresh single-thread
//! process before exec. Preventing process creation closes the setsid/double-fork
//! escape race; library/Node worker threads remain available.
use std::{ffi::OsString, os::unix::process::CommandExt};

pub const HELPER_ARG: &str = "__grv_supervised_helper";

#[cfg(target_os = "macos")]
fn install() -> std::io::Result<()> {
    unsafe extern "C" {
        fn sandbox_init(
            profile: *const libc::c_char,
            flags: u64,
            error: *mut *mut libc::c_char,
        ) -> libc::c_int;
        fn sandbox_free_error(error: *mut libc::c_char);
    }
    let mut error = std::ptr::null_mut();
    let status = unsafe {
        sandbox_init(
            c"(version 1) (allow default) (deny process-fork)".as_ptr(),
            0,
            &mut error,
        )
    };
    if !error.is_null() {
        unsafe { sandbox_free_error(error) };
    }
    if status != 0 {
        return Err(std::io::Error::other("helper containment unavailable"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn install() -> std::io::Result<()> {
    const LD: u16 = 0x20;
    const JEQ: u16 = 0x15;
    const JSET: u16 = 0x45;
    const RET: u16 = 0x06;
    const ALLOW: u32 = 0x7fff0000;
    const DENY: u32 = 0x00050000 | libc::EPERM as u32;
    #[cfg(target_arch = "x86_64")]
    const ARCH: u32 = 0xc000003e;
    #[cfg(target_arch = "aarch64")]
    const ARCH: u32 = 0xc00000b7;
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    compile_error!("Salesforce containment supports x86-64 and ARM64");
    fn instruction(code: u16, jt: u8, jf: u8, k: u32) -> libc::sock_filter {
        libc::sock_filter { code, jt, jf, k }
    }
    let mut filter = vec![
        instruction(LD, 0, 0, 4),
        instruction(JEQ, 1, 0, ARCH),
        instruction(RET, 0, 0, DENY),
        instruction(LD, 0, 0, 0),
    ];
    #[cfg(target_arch = "x86_64")]
    for syscall in [libc::SYS_fork, libc::SYS_vfork] {
        filter.push(instruction(JEQ, 0, 1, syscall as u32));
        filter.push(instruction(RET, 0, 0, DENY));
    }
    #[cfg(target_arch = "x86_64")]
    {
        // x32 uses the same audit arch with a syscall-number bit; never permit
        // its alternate process-creation ABI to bypass native syscall checks.
        filter.push(instruction(JSET, 0, 1, 0x40000000));
        filter.push(instruction(RET, 0, 0, DENY));
    }
    for syscall in [
        libc::SYS_setsid,
        libc::SYS_setpgid,
        libc::SYS_unshare,
        libc::SYS_setns,
    ] {
        filter.push(instruction(JEQ, 0, 1, syscall as u32));
        filter.push(instruction(RET, 0, 0, DENY));
    }
    // libc pthread implementations can fall back from clone3 to clone.
    filter.push(instruction(JEQ, 0, 1, libc::SYS_clone3 as u32));
    filter.push(instruction(RET, 0, 0, 0x00050000 | libc::ENOSYS as u32));
    filter.push(instruction(JEQ, 0, 4, libc::SYS_clone as u32));
    filter.push(instruction(LD, 0, 0, 16)); // low 32 bits of clone(flags)
    filter.push(instruction(JSET, 1, 0, libc::CLONE_THREAD as u32));
    filter.push(instruction(RET, 0, 0, DENY));
    filter.push(instruction(RET, 0, 0, ALLOW));
    filter.push(instruction(RET, 0, 0, ALLOW));
    let program = libc::sock_fprog {
        len: filter.len() as u16,
        filter: filter.as_mut_ptr(),
    };
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0
        || unsafe { libc::prctl(libc::PR_SET_SECCOMP, libc::SECCOMP_MODE_FILTER, &program) } != 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn install() -> std::io::Result<()> {
    Err(std::io::Error::other("helper containment unsupported"))
}

pub fn exec(args: impl Iterator<Item = OsString>) -> ! {
    let mut args = args;
    let Some(program) = args.next() else {
        std::process::exit(126)
    };
    // Close inherited protocol/GRV handles before loading either helper. This
    // fresh process is single-threaded and has no operation-owned descriptors.
    #[cfg(target_os = "macos")]
    close_inherited();
    #[cfg(target_os = "linux")]
    {
        let result = unsafe { libc::syscall(libc::SYS_close_range, 3u32, u32::MAX, 0u32) };
        if result != 0 {
            close_inherited();
        }
    }
    if install().is_err() {
        eprintln!("Salesforce helper containment failed");
        std::process::exit(126)
    };
    let _error = std::process::Command::new(program).args(args).exec();
    eprintln!("Salesforce contained helper could not execute");
    std::process::exit(126)
}

fn close_inherited() {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0
        || limit.rlim_cur > 1_048_576
    {
        eprintln!("Salesforce helper descriptor containment unavailable");
        std::process::exit(126);
    }
    for fd in 3..limit.rlim_cur {
        unsafe { libc::close(fd as i32) };
    }
}
