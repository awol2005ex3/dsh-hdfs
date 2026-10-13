//! 与 TS 侧共享的纯数据结构（napi 支持 struct <-> JS object 自动转换）。

use napi_derive::napi;
use serde::{Deserialize, Serialize};

/// 一个 HDFS 集群连接的完整配置。
///
/// `url` 形如 `hdfs://nameservice1`（HA）或 `hdfs://host:8020`（单 NN）。
/// `extra_config` 是 Hadoop 配置项的 JSON 字符串，例如：
/// ```json
/// {
///   "dfs.nameservices": "nameservice1",
///   "dfs.namenode.rpc-address.nameservice1.namenode1": "node1:8020",
///   "dfs.ha.namenodes.nameservice1": "namenode1,namenode2",
///   "dfs.client.failover.proxy.provider.nameservice1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
///   "dfs.namenode.kerberos.principal": "hdfs/_HOST@REALM.COM",
///   "dfs.namenode.keytab.file": "C:/kerberos/hdfs.keytab"
/// }
/// ```
///
/// 用户名来源（按优先级）：
/// 1. Kerberos：`dfs.namenode.kerberos.principal` 的 `@` 前半段（即 `principal` 的 user 部分）；
/// 2. 非 Kerberos：环境变量 `HADOOP_USER_NAME` → `HADOOP_USERNAME` → `HDFS_USERNAME`。
#[napi(object)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HdfsConfig {
    /// `hdfs://nameservice1` 或 `hdfs://host:8020`
    pub url: String,
    /// Hadoop 扩展配置（JSON 字符串）。空字符串视为无扩展配置。
    pub extra_config: String,
    /// 连接超时（毫秒），0 用默认值
    pub timeout_ms: i64,
}

/// HDFS 文件 / 目录条目。字段与 hdfs-gui 的 `HdfsFile` 保持一致，便于复用前端逻辑。
#[napi(object)]
#[derive(Debug, Clone, Serialize)]
pub struct HdfsFile {
    pub name: String,
    pub path: String,
    pub parent_path: String,
    pub owner: String,
    pub group: String,
    pub isdir: bool,
    /// 八进制权限位，如 0o755 = 493
    pub permission: u32,
    pub modification_time: i64,
    pub access_time: i64,
    pub length: i64,
    /// 目录内容摘要（仅目录且调用方勾选时填充）
    pub file_count: Option<i64>,
    pub directory_count: Option<i64>,
    pub quota: Option<i64>,
    pub space_consumed: Option<i64>,
    pub space_quota: Option<i64>,
}

/// 文件内容预览 / 完整内容。
#[napi(object)]
#[derive(Debug, Clone, Serialize)]
pub struct HdfsFileContent {
    pub length: i64,
    /// UTF-8 有损解码后的文本（二进制文件会含替换符）
    pub content: String,
    /// 是否 ORC 文件（魔数 "ORC"）
    pub isorc: bool,
    /// 是否 Parquet 文件（魔数 "PAR1"）
    pub isparquet: bool,
    /// 是否 Avro 文件（按扩展名判断）
    pub isavro: bool,
}

/// ACL 条目。
#[napi(object)]
#[derive(Debug, Clone, Serialize)]
pub struct HdfsAclEntry {
    /// user | group | mask | other
    pub rtype: String,
    /// access | default
    pub scope: String,
    /// rwx 形式的权限串
    pub permissions: String,
    pub name: Option<String>,
}

/// 文件的完整 ACL 状态。
#[napi(object)]
#[derive(Debug, Clone, Serialize)]
pub struct HdfsAcl {
    pub owner: String,
    pub group: String,
    pub sticky: bool,
    pub entries: Vec<HdfsAclEntry>,
    pub permission: u32,
    pub file_path: String,
    pub isdir: bool,
}

/// 特殊目录（垃圾箱 / staging）快速查找结果。
#[napi(object)]
#[derive(Debug, Clone, Serialize)]
pub struct FindDirsResult {
    pub dirs: Vec<HdfsFile>,
    /// 已访问（list 过）的目录数
    pub visited: i64,
    /// 是否因达到上限而提前结束
    pub truncated: bool,
    /// 是否因达到最大深度而停止（仍有未展开的目录）
    pub stopped_at_max_depth: bool,
    /// 前若干条错误信息
    pub errors: Vec<String>,
}

/// Kerberos 登录结果。
#[napi(object)]
#[derive(Debug, Clone, Serialize)]
pub struct KerberosLoginResult {
    pub ok: bool,
    /// 使用的 principal（`user@REALM`）
    pub principal: String,
    /// ccache 文件路径
    pub ccache_path: String,
    /// 说明信息
    pub message: String,
}

/// Kerberos 凭据状态探测结果。
#[napi(object)]
#[derive(Debug, Clone, Serialize)]
pub struct KerberosStatus {
    pub ok: bool,
    pub message: String,
}

/// 一次「列出目录」的结果。
#[napi(object)]
#[derive(Debug, Clone, Serialize)]
pub struct ListResult {
    pub path: String,
    pub files: Vec<HdfsFile>,
    /// 目录自身信息（不存在时为 None）
    pub self_info: Option<HdfsFile>,
}
