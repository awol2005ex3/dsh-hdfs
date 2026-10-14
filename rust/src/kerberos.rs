//! Kerberos 认证：纯 Rust 实现，替代 MIT kinit 与系统 libgssapi_krb5。
//!
//! 背景：
//! - `hdfs-native` 的 SASL/GSS 通过 `libloading` 动态加载 `libgssapi_krb5`（Windows 上是
//!   `gssapi_krb5.dll`），本机通常没有，所以 Kerberos 协商会失败。
//! - `hdfs-gui` 的做法（见 `src-tauri/src/commands/hdfs_config.rs::init_connection`）是用
//!   纯 Rust 的 `kinit-kt` 从 keytab 取 TGT，把 ccache 写到文件并设 `KRB5CCNAME`。
//!
//! 这里沿用同一策略：`kerberos_login()` 负责写 ccache + 设置进程级 `KRB5CCNAME`；
//! `kerberos_status()` 供 TS 侧探测「是否已就绪」。

use std::path::PathBuf;

/// ccache 文件名（放在本机用户目录下，避免与 CWD 耦合）。
const CCACHE_NAME: &str = "dsh_hdfs_ccache";

/// 解析出 ccache 的目标路径。
///
/// 环境变量 `KRB5CCNAME` 只在 `FILE:` 形态下被沿用：Windows 上它常被其他工具
/// （JDK / MIT KFW / Hadoop 客户端）设成 `API:krb5cc` 这类非文件型 ccache 名，
/// hdfs-native 经系统 GSS 库解析不了它——照抄只会把凭据写进谁也读不到的地方
/// （`API:krb5cc` 在 NTFS 上还是文件 `API` 的备用数据流）。因此一律退到本插件
/// 自己的固定文件路径，保证 ccache 落点确定、可被系统 GSS 读取。
fn ccache_path() -> PathBuf {
    if let Ok(existing) = std::env::var("KRB5CCNAME") {
        let trimmed = existing.trim();
        if let Some(bare) = trimmed.strip_prefix("FILE:") {
            let bare = bare.trim();
            if !bare.is_empty() {
                return PathBuf::from(bare);
            }
        }
    }
    let mut dir = dirs_home();
    dir.push(CCACHE_NAME);
    dir
}

/// 用户主目录（跨平台，不引入 dirs crate：用 HOME / USERPROFILE 两个约定变量）。
fn dirs_home() -> PathBuf {
    for key in ["HOME", "USERPROFILE"] {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return PathBuf::from(trimmed);
            }
        }
    }
    std::env::temp_dir()
}

/// 用 keytab + principal 获取 TGT 并写入 ccache。
///
/// 返回 `(principal, ccache_path, message)`。
pub fn kerberos_login(keytab: &str, principal: &str) -> Result<(String, String, String), String> {
    if keytab.trim().is_empty() {
        return Err("dfs.namenode.keytab.file 未配置".into());
    }
    if principal.trim().is_empty() {
        return Err("dfs.namenode.kerberos.principal 未配置".into());
    }
    if !std::path::Path::new(keytab).exists() {
        return Err(format!("keytab 文件不存在: {keytab}"));
    }

    let cache_file_path = ccache_path();

    // 先清掉旧的 ccache，避免读到过期票据
    let _ = std::fs::remove_file(&cache_file_path);

    // kinit-kt 返回 FILE 格式的 ccache 字节
    let ccache_data = kinit_kt::request_tgt(keytab, principal, None)
        .map_err(|e| format!("kinit 失败（principal={principal}）: {e}"))?;

    std::fs::write(&cache_file_path, &ccache_data)
        .map_err(|e| format!("写入 ccache 失败 ({}): {e}", cache_file_path.display()))?;

    // hdfs-native 的 GSS 侧走系统库读取凭证；同时把路径写进环境变量，
    // 让同进程内后续连接（以及 MIT 系工具）都能找到。
    // 注意：Rust 2024 里 std::env::set_var 是 unsafe，这里明确标注。
    unsafe {
        std::env::set_var("KRB5CCNAME", cache_file_path.to_string_lossy().to_string());
    }

    Ok((
        principal.to_string(),
        cache_file_path.to_string_lossy().to_string(),
        format!(
            "Kerberos TGT 已获取并写入 {}。注意：hdfs-native 的 SASL 协商仍需要系统 gssapi_krb5 动态库，Windows 上通常缺失——详见 README。",
            cache_file_path.display()
        ),
    ))
}

/// 探测当前是否已具备 Kerberos 凭据（ccache 存在且非空）。
pub fn kerberos_status() -> (bool, String) {
    let path = ccache_path();
    match std::fs::metadata(&path) {
        Ok(meta) if meta.len() > 0 => (
            true,
            format!("ccache 已就绪: {} ({} bytes)", path.display(), meta.len()),
        ),
        _ => (
            false,
            format!("ccache 缺失或为空: {}（请先执行 kerberos_login）", path.display()),
        ),
    }
}
