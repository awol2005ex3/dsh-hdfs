//! dsh-hdfs 的原生半：把 `hdfs-native` 的能力以 napi 函数暴露给 Node。
//!
//! 设计要点：
//! - **不做全局 client 缓存**：每个函数都新建 `hdfs_native::Client`。hdfs-native 的
//!   Client 内部是 thrift + 连接池，重建开销主要在 TCP 与一次 RPC 握手，换取「无状态、
//!   无泄漏、无并发锁」对插件这种长驻进程更安全。
//! - **异步**：所有 I/O 走 napi 的 tokio runtime feature，函数返回 Promise。
//! - **错误**：一律 `Result<T, napi::Error>`，message 直接透传底层错误（与 hdfs-gui
//!   的 `map_err(|e| e.to_string())` 一致），浏览器半展示即可。

use std::collections::HashMap;
use std::path::Path;

use futures_util::StreamExt;
use hdfs_native::{Client, WriteOptions};
use napi::bindgen_prelude::*;
use napi_derive::napi;

mod kerberos;
mod types;

use crate::kerberos::{kerberos_login, kerberos_status};
use crate::types::{
    FindDirsResult, HdfsAcl, HdfsAclEntry, HdfsConfig, HdfsFile, HdfsFileContent, KerberosLoginResult,
    KerberosStatus, ListResult,
};

/// 从 `extra_config` JSON 解析出 hdfs-native 需要的 `HashMap<String, String>`。
fn parse_extra(raw: &str) -> HashMap<String, String> {
    if raw.trim().is_empty() {
        return HashMap::new();
    }
    match serde_json::from_str::<HashMap<String, serde_json::Value>>(raw) {
        Ok(map) => map
            .into_iter()
            .filter_map(|(k, v)| {
                // Hadoop 配置值都是字符串；数字/布尔也转成字符串
                let s = match v {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                };
                Some((k, s))
            })
            .collect(),
        Err(e) => {
            // 配置写错不应静默——但也不该让整个调用失败，打日志后按无扩展配置跑
            eprintln!("dsh-hdfs-native: extra_config 不是合法 JSON: {e}");
            HashMap::new()
        }
    }
}

/// 按配置新建一个 hdfs-native Client。
///
/// 用户名的两种来源（见 `resolve_user`）：
/// - Kerberos：principal 的 `@` 前半段；
/// - 非 Kerberos：环境变量 `HADOOP_USER_NAME` / `HADOOP_USERNAME` / `HDFS_USERNAME`。
///
/// hdfs-native 读取用户名只认 `HADOOP_USER_NAME` 环境变量，所以这里在解析后统一
/// 回写进去，保证 Kerberos / 非 Kerberos 两条路径都能生效。
fn build_client(config: &HdfsConfig) -> std::result::Result<Client, String> {
    let other_config = parse_extra(&config.extra_config);
    let url = config.url.trim();
    if url.is_empty() {
        return Err("hdfs url 未配置".into());
    }
    if !url.starts_with("hdfs://") {
        return Err(format!("hdfs url 必须以 hdfs:// 开头: {url}"));
    }

    let user = resolve_user(config);
    // Rust 2024 里 std::env::set_var 是 unsafe，明确标注。
    if !user.is_empty() {
        unsafe {
            std::env::set_var("HADOOP_USER_NAME", user.clone());
        }
    }

    Client::new_with_config(url, other_config).map_err(|e| e.to_string())
}

/// `FileStatus` → `HdfsFile`。
fn to_hdfs_file(file: &hdfs_native::client::FileStatus, parent_path: &str) -> HdfsFile {
    let name = Path::new(&file.path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_string();
    let parent = Path::new(&file.path)
        .parent()
        .and_then(|s| s.to_str())
        .unwrap_or(parent_path)
        .to_string();
    HdfsFile {
        name,
        path: file.path.replace('\\', "/"),
        parent_path: parent.replace('\\', "/"),
        owner: file.owner.clone(),
        group: file.group.clone(),
        isdir: file.isdir,
        permission: file.permission as u32,
        modification_time: file.modification_time as i64,
        access_time: file.access_time as i64,
        length: file.length as i64,
        file_count: None,
        directory_count: None,
        quota: None,
        space_consumed: None,
        space_quota: None,
    }
}

/// 从 extra_config 里取 Kerberos 配置；两者都存在才启用。
fn kerberos_pair(config: &HdfsConfig) -> Option<(String, String)> {
    let map = parse_extra(&config.extra_config);
    let principal = map.get("dfs.namenode.kerberos.principal")?.clone();
    let keytab = map.get("dfs.namenode.keytab.file")?.clone();
    if principal.trim().is_empty() || keytab.trim().is_empty() {
        return None;
    }
    Some((keytab, principal))
}

/// 探测插件的原生半是否可用（浏览器/宿主半启动时调用一次）。
#[napi]
pub fn native_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// 执行 Kerberos 登录（写 ccache + 设 KRB5CCNAME）。
#[napi]
pub async fn kerberos_login_with_config(config: HdfsConfig) -> Result<KerberosLoginResult> {
    let (keytab, principal) = kerberos_pair(&config)
        .ok_or_else(|| Error::new(Status::InvalidArg, "extra_config 缺少 dfs.namenode.kerberos.principal / dfs.namenode.keytab.file".to_string()))?;
    let (principal, ccache_path, message) = kerberos_login(&keytab, &principal)
        .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    Ok(KerberosLoginResult {
        ok: true,
        principal,
        ccache_path,
        message,
    })
}

/// 查询 ccache 状态。
#[napi]
pub fn kerberos_status_check() -> KerberosStatus {
    let (ok, message) = kerberos_status();
    KerberosStatus { ok, message }
}

/// 列出目录内容。`show_content_summary` 为 true 时额外对每个子目录取内容摘要。
#[napi]
pub async fn list_status(
    config: HdfsConfig,
    path: String,
    show_content_summary: bool,
) -> Result<ListResult> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let normalized = normalize_path(&path);

    let self_info = match client.get_file_info(&normalized).await {
        Ok(status) => Some(to_hdfs_file(&status, &normalized)),
        Err(_) => None,
    };

    let files = client
        .list_status(&normalized, false)
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("list {}: {e}", normalized)))?;

    let parent = normalized.clone();
    let mut result: Vec<HdfsFile> = files.iter().map(|f| to_hdfs_file(f, &parent)).collect();

    if show_content_summary {
        for entry in result.iter_mut() {
            if !entry.isdir {
                continue;
            }
            match client.get_content_summary(&entry.path).await {
                Ok(cs) => {
                    entry.length = cs.length as i64;
                    entry.file_count = Some(cs.file_count as i64);
                    entry.directory_count = Some(cs.directory_count as i64);
                    entry.quota = Some(cs.quota as i64);
                    entry.space_consumed = Some(cs.space_consumed as i64);
                    entry.space_quota = Some(cs.space_quota as i64);
                }
                Err(e) => eprintln!("dsh-hdfs-native: content summary {} failed: {e}", entry.path),
            }
        }
    }

    Ok(ListResult {
        path: normalized,
        files: result,
        self_info,
    })
}

/// 取单个文件 / 目录的状态。
#[napi]
pub async fn get_file_status(config: HdfsConfig, path: String) -> Result<HdfsFile> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let normalized = normalize_path(&path);
    let status = client
        .get_file_info(&normalized)
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("stat {}: {e}", normalized)))?;
    Ok(to_hdfs_file(&status, &normalized))
}

/// 读取文件内容（默认上限 8 MiB，`max_bytes` 可调；0 表示不限制）。
#[napi]
pub async fn read_file(config: HdfsConfig, path: String, max_bytes: i64) -> Result<HdfsFileContent> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let normalized = normalize_path(&path);

    let status = client
        .get_file_info(&normalized)
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("stat {}: {e}", normalized)))?;

    let mut reader = client
        .read(&normalized)
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("open {}: {e}", normalized)))?;

    let limit = if max_bytes > 0 { max_bytes as usize } else { status.length as usize };
    let want = (limit as u64).min(status.length as u64) as usize;
    let buf = reader
        .read(want)
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("read {}: {e}", normalized)))?;

    let content = String::from_utf8_lossy(&buf).to_string();
    Ok(HdfsFileContent {
        length: status.length as i64,
        content,
        isorc: buf.starts_with(b"ORC"),
        isparquet: buf.starts_with(b"PAR1"),
        isavro: normalized.ends_with(".avro"),
    })
}

/// 写文本到 HDFS（覆盖写）。
#[napi]
pub async fn write_text(config: HdfsConfig, path: String, content: String) -> Result<bool> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let normalized = normalize_path(&path);
    let mut writer = client
        .create(&normalized, WriteOptions::default().overwrite(true))
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("create {}: {e}", normalized)))?;
    writer
        .write(bytes::Bytes::copy_from_slice(content.as_bytes()))
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("write {}: {e}", normalized)))?;
    writer
        .close()
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("close {}: {e}", normalized)))?;
    Ok(true)
}

/// 上传本地文件到 HDFS。
#[napi]
pub async fn upload_file(config: HdfsConfig, local_path: String, remote_dir: String) -> Result<String> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let file_name = Path::new(&local_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_string();
    if file_name.is_empty() {
        return Err(Error::new(Status::InvalidArg, format!("无法从本地路径解析文件名: {local_path}")));
    }
    let dir = normalize_path(&remote_dir);
    let remote = format!("{}/{}", dir.trim_end_matches('/'), file_name);

    let file = std::fs::File::open(&local_path)
        .map_err(|e| Error::new(Status::GenericFailure, format!("打开本地文件 {}: {e}", local_path)))?;
    let mut reader = std::io::BufReader::new(file);

    let mut writer = client
        .create(&remote, WriteOptions::default())
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("create {}: {e}", remote)))?;

    loop {
        let mut buf = [0u8; 64 * 1024];
        let n = match std::io::Read::read(&mut reader, &mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                let _ = writer.close().await;
                return Err(Error::new(Status::GenericFailure, format!("读本地文件: {e}")));
            }
        };
        writer
            .write(bytes::Bytes::copy_from_slice(&buf[..n]))
            .await
            .map_err(|e| Error::new(Status::GenericFailure, format!("write {}: {e}", remote)))?;
    }
    writer
        .close()
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("close {}: {e}", remote)))?;
    Ok(remote)
}

/// 下载 HDFS 文件到本地目录。
#[napi]
pub async fn download_file(config: HdfsConfig, remote_path: String, local_dir: String) -> Result<String> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let normalized = normalize_path(&remote_path);
    let file_name = Path::new(&normalized)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("download.bin")
        .to_string();

    let local = Path::new(&local_dir).join(&file_name);
    let mut out = std::fs::File::create(&local)
        .map_err(|e| Error::new(Status::GenericFailure, format!("创建本地文件 {}: {e}", local.display())))?;

    let mut reader = client
        .read(&normalized)
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("open {}: {e}", normalized)))?;

    loop {
        let chunk = reader
            .read(256 * 1024)
            .await
            .map_err(|e| Error::new(Status::GenericFailure, format!("read {}: {e}", normalized)))?;
        if chunk.is_empty() {
            break;
        }
        std::io::Write::write_all(&mut out, &chunk)
            .map_err(|e| Error::new(Status::GenericFailure, format!("写本地文件: {e}")))?;
    }
    Ok(local.to_string_lossy().to_string())
}

/// 删除文件 / 目录。`to_trash` 为 true 时移入 `/user/<user>/.Trash/Current`。
#[napi]
pub async fn delete_paths(config: HdfsConfig, paths: Vec<String>, to_trash: bool) -> Result<i64> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let mut removed: i64 = 0;

    if to_trash {
        let user = resolve_user(&config);
        let trash = format!("/user/{}/.Trash/Current", user);
        for raw in paths {
            let p = normalize_path(&raw);
            let target = format!("{}{}", trash, p);
            if let Some(parent) = Path::new(&target).parent() {
                let _ = client.mkdirs(parent.to_str().unwrap_or_default(), 0o755, true).await;
            }
            client
                .rename(&p, &target, true)
                .await
                .map_err(|e| Error::new(Status::GenericFailure, format!("移入垃圾箱 {}: {e}", p)))?;
            removed += 1;
        }
    } else {
        for raw in paths {
            let p = normalize_path(&raw);
            client
                .delete(&p, true)
                .await
                .map_err(|e| Error::new(Status::GenericFailure, format!("删除 {}: {e}", p)))?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// 新建目录。
#[napi]
pub async fn mkdirs(config: HdfsConfig, path: String) -> Result<bool> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let normalized = normalize_path(&path);
    client
        .mkdirs(&normalized, 0o755, false)
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("mkdirs {}: {e}", normalized)))?;
    Ok(true)
}

/// 新建空白文件。
#[napi]
pub async fn create_empty_file(config: HdfsConfig, path: String) -> Result<bool> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let normalized = normalize_path(&path);
    let mut writer = client
        .create(&normalized, WriteOptions::default())
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("create {}: {e}", normalized)))?;
    writer
        .close()
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("close {}: {e}", normalized)))?;
    Ok(true)
}

/// 重命名 / 移动。
#[napi]
pub async fn rename_path(config: HdfsConfig, old_path: String, new_name: String) -> Result<String> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let old = normalize_path(&old_path);
    let parent = Path::new(&old)
        .parent()
        .and_then(|s| s.to_str())
        .unwrap_or("/");
    let new_path = format!("{}/{}", parent.trim_end_matches('/'), new_name.trim());
    client
        .rename(&old, &new_path, true)
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("rename {} -> {}: {e}", old, new_path)))?;
    Ok(new_path)
}

/// 设置权限（八进制，如 493 = 0o755）。`recursive` 时递归子项。
#[napi]
pub async fn set_permission(
    config: HdfsConfig,
    paths: Vec<String>,
    permission: u32,
    recursive: bool,
) -> Result<i64> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let mut changed: i64 = 0;
    for raw in paths {
        let p = normalize_path(&raw);
        client
            .set_permission(&p, permission)
            .await
            .map_err(|e| Error::new(Status::GenericFailure, format!("chmod {}: {e}", p)))?;
        changed += 1;
        if recursive {
            let dir = client.list_status_iter(&p, true);
            futures_util::pin_mut!(dir);
            while let Some(entry) = dir.next().await {
                let entry = entry
                    .map_err(|e| Error::new(Status::GenericFailure, format!("遍历 {}: {e}", p)))?;
                let child = entry.path.replace('\\', "/");
                client
                    .set_permission(&child, permission)
                    .await
                    .map_err(|e| Error::new(Status::GenericFailure, format!("chmod {}: {e}", child)))?;
                changed += 1;
            }
        }
    }
    Ok(changed)
}

/// 取 ACL 状态。
#[napi]
pub async fn get_acl_status(config: HdfsConfig, path: String) -> Result<HdfsAcl> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let normalized = normalize_path(&path);
    let status = client
        .get_file_info(&normalized)
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("stat {}: {e}", normalized)))?;
    let acl = client
        .get_acl_status(&normalized)
        .await
        .map_err(|e| Error::new(Status::GenericFailure, format!("getacl {}: {e}", normalized)))?;
    Ok(HdfsAcl {
        owner: acl.owner,
        group: acl.group,
        sticky: acl.sticky,
        entries: acl
            .entries
            .into_iter()
            .map(|e| HdfsAclEntry {
                rtype: e.r#type.to_string(),
                scope: e.scope.to_string(),
                permissions: e.permissions.to_string(),
                name: e.name,
            })
            .collect(),
        permission: acl.permission as u32,
        file_path: normalized,
        isdir: status.isdir,
    })
}

/// 增删 ACL 条目。
#[napi]
pub async fn modify_acl_entries(
    config: HdfsConfig,
    path: String,
    entries: Vec<HdfsAclEntry>,
    remove: bool,
) -> Result<bool> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let normalized = normalize_path(&path);
    let specs: Vec<hdfs_native::acl::AclEntry> = entries
        .into_iter()
        .map(|e| hdfs_native::acl::AclEntry::new(e.rtype, e.scope, e.permissions, e.name))
        .collect();
    if specs.is_empty() {
        return Err(Error::new(Status::InvalidArg, "没有要操作的 ACL 条目"));
    }
    if remove {
        client
            .remove_acl_entries(&normalized, specs)
            .await
            .map_err(|e| Error::new(Status::GenericFailure, format!("remove acl {}: {e}", normalized)))?;
    } else {
        client
            .modify_acl_entries(&normalized, specs)
            .await
            .map_err(|e| Error::new(Status::GenericFailure, format!("modify acl {}: {e}", normalized)))?;
    }
    Ok(true)
}

/// 删除 default ACL / 全部 ACL。
#[napi]
pub async fn remove_acl(config: HdfsConfig, path: String, only_default: bool) -> Result<bool> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let normalized = normalize_path(&path);
    if only_default {
        client
            .remove_default_acl(&normalized)
            .await
            .map_err(|e| Error::new(Status::GenericFailure, format!("remove default acl {}: {e}", normalized)))?;
    } else {
        client
            .remove_acl(&normalized)
            .await
            .map_err(|e| Error::new(Status::GenericFailure, format!("remove acl {}: {e}", normalized)))?;
    }
    Ok(true)
}

/// 按名称模式快速查找目录（垃圾箱 / staging），支持深度、并发与上限控制。
#[napi]
#[allow(clippy::too_many_arguments)]
pub async fn find_special_dirs(
    config: HdfsConfig,
    root_path: String,
    patterns: Vec<String>,
    max_depth: i64,
    concurrency: i64,
    max_visited: i64,
    max_results: i64,
    show_content_summary: bool,
) -> Result<FindDirsResult> {
    let patterns: Vec<String> = patterns
        .iter()
        .map(|p| p.trim().to_lowercase())
        .filter(|p| !p.is_empty())
        .collect();
    if patterns.is_empty() {
        return Err(Error::new(Status::InvalidArg, "patterns 不能为空"));
    }

    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    let root = {
        let p = root_path.replace('\\', "/");
        let p = p.trim_end_matches('/');
        if p.is_empty() { "/".to_string() } else { p.to_string() }
    };
    let concurrency = concurrency.clamp(1, 32) as usize;
    let max_depth = max_depth.clamp(1, 12) as usize;
    let max_visited = max_visited.clamp(100, 100_000) as usize;
    let max_results = max_results.clamp(1, 5000) as usize;

    let mut result = FindDirsResult {
        dirs: Vec::new(),
        visited: 0,
        truncated: false,
        stopped_at_max_depth: false,
        errors: Vec::new(),
    };

    let mut frontier = vec![root];
    let client_ref = &client;

    'levels: for _ in 0..max_depth {
        if frontier.is_empty() {
            break;
        }
        let mut next_frontier: Vec<String> = Vec::new();
        let chunk_size = (concurrency * 8).max(16);

        for chunk in frontier.chunks(chunk_size) {
            let budget = max_visited.saturating_sub(result.visited as usize);
            if budget == 0 {
                result.truncated = true;
                break 'levels;
            }
            let chunk = &chunk[..chunk.len().min(budget)];

            let mut stream = futures_util::stream::iter(
                chunk
                    .iter()
                    .cloned()
                    .map(move |path| async move { (path.clone(), client_ref.list_status(&path, false).await) }),
            )
            .buffer_unordered(concurrency);

            while let Some((path, res)) = stream.next().await {
                result.visited += 1;
                match res {
                    Ok(files) => {
                        for file in files.iter() {
                            if !file.isdir {
                                continue;
                            }
                            let entry_path = file.path.replace('\\', "/");
                            let name = Path::new(&entry_path)
                                .file_name()
                                .and_then(|s| s.to_str())
                                .unwrap_or_default()
                                .to_lowercase();
                            if patterns.iter().any(|p| name.contains(p.as_str())) {
                                if result.dirs.len() < max_results {
                                    result.dirs.push(to_hdfs_file(file, &entry_path));
                                } else {
                                    result.truncated = true;
                                }
                            } else {
                                next_frontier.push(entry_path);
                            }
                        }
                    }
                    Err(e) => {
                        if result.errors.len() < 20 {
                            result.errors.push(format!("{path}: {e}"));
                        }
                    }
                }
                if result.visited as usize >= max_visited {
                    result.truncated = true;
                    break;
                }
            }
            if result.truncated {
                break 'levels;
            }
        }
        frontier = next_frontier;
    }

    result.stopped_at_max_depth = !result.truncated && !frontier.is_empty();

    if show_content_summary {
        let paths: Vec<String> = result.dirs.iter().map(|d| d.path.clone()).collect();
        let mut stream = futures_util::stream::iter(
            paths
                .into_iter()
                .enumerate()
                .map(move |(index, path)| async move { (index, client_ref.get_content_summary(&path).await) }),
        )
        .buffer_unordered(concurrency);
        while let Some((index, summary)) = stream.next().await {
            if let Ok(cs) = summary {
                if let Some(dir) = result.dirs.get_mut(index) {
                    dir.length = cs.length as i64;
                    dir.file_count = Some(cs.file_count as i64);
                    dir.directory_count = Some(cs.directory_count as i64);
                    dir.quota = Some(cs.quota as i64);
                    dir.space_consumed = Some(cs.space_consumed as i64);
                    dir.space_quota = Some(cs.space_quota as i64);
                }
            }
        }
    }

    Ok(result)
}

/// 探测连通性：列根目录或 /user，返回一条条目即视为可用。
#[napi]
pub async fn ping(config: HdfsConfig) -> Result<bool> {
    let client = build_client(&config).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    match client.list_status("/", false).await {
        Ok(_) => Ok(true),
        Err(e) => Err(Error::new(Status::GenericFailure, format!("连接失败: {e}"))),
    }
}

// ── 内部工具 ────────────────────────────────────────────────────────────────

/// 规范化 HDFS 路径：统一分隔符、保证以 `/` 开头、去掉尾部 `/`（根目录除外）。
fn normalize_path(raw: &str) -> String {
    let p = raw.replace('\\', "/");
    if p.is_empty() {
        return "/".to_string();
    }
    let with_root = if p.starts_with('/') { p } else { format!("/{p}") };
    let trimmed = with_root.trim_end_matches('/');
    if trimmed.is_empty() { "/".to_string() } else { trimmed.to_string() }
}

/// 解析本次连接使用的用户名。
///
/// 两种情况：
/// 1. **Kerberos**：取 `dfs.namenode.kerberos.principal` 中 `@` 前半段
///    （`hdfs/node1@REALM` → `hdfs`；`user@REALM` → `user`）。
/// 2. **非 Kerberos**：依次读 `HADOOP_USER_NAME` / `HADOOP_USERNAME` / `HDFS_USERNAME`。
///
/// 都取不到时返回空串，交给 hdfs-native 自己用系统用户名兜底。
fn resolve_user(config: &HdfsConfig) -> String {
    let map = parse_extra(&config.extra_config);
    if let Some(principal) = map.get("dfs.namenode.kerberos.principal") {
        // Kerberos 情况：principal 参数优先，取 realm 前的 user 部分。
        // 同时兼容 `hdfs/_HOST@REALM`（服务主体）与 `user@REALM`（用户主体）。
        let user = principal
            .split('@')
            .next()
            .unwrap_or_default()
            .split('/')
            .next()
            .unwrap_or_default()
            .trim();
        if !user.is_empty() {
            return user.to_string();
        }
    }
    // 非 Kerberos 情况：环境变量。
    for key in ["HADOOP_USER_NAME", "HADOOP_USERNAME", "HDFS_USERNAME"] {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }
    String::new()
}
