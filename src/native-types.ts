/**
 * 原生模块（rust/ → lib/native/index.node）的手写类型声明。
 *
 * 与 `rust/src/types.rs` 的 napi 导出一一对应；由 `src/native.ts` 在宿主半
 * 惰性加载 `lib/native/index.js`（CJS 绑定）后以该类型使用。
 *
 * 若用 `napi build --dts` 生成了官方声明，可以此文件为准做交叉核对；
 * 运行时形状由 Rust 侧 `#[napi]` 宏保证与这里一致。
 */

/** 一个 HDFS 集群连接的完整配置。 */
export interface HdfsConfig {
  /** `hdfs://nameservice1` 或 `hdfs://host:8020` */
  url: string
  /** Hadoop 扩展配置（JSON 字符串），含 HA / Kerberos 参数 */
  extraConfig: string
  /** 连接超时（毫秒），0 用默认值 */
  timeoutMs: number
}

/** HDFS 文件 / 目录条目。 */
export interface HdfsFile {
  name: string
  path: string
  parentPath: string
  owner: string
  group: string
  isdir: boolean
  /** 八进制权限位，如 0o755 = 493 */
  permission: number
  modificationTime: number
  accessTime: number
  length: number
  /** 目录内容摘要（仅目录且调用方勾选时填充） */
  fileCount?: number | null
  directoryCount?: number | null
  quota?: number | null
  spaceConsumed?: number | null
  spaceQuota?: number | null
}

/** 文件内容预览 / 完整内容。 */
export interface HdfsFileContent {
  length: number
  /** UTF-8 有损解码后的文本 */
  content: string
  isorc: boolean
  isparquet: boolean
  isavro: boolean
}

/** ACL 条目。 */
export interface HdfsAclEntry {
  /** user | group | mask | other */
  rtype: string
  /** access | default */
  scope: string
  /** rwx 形式的权限串 */
  permissions: string
  name?: string | null
}

/** 文件的完整 ACL 状态。 */
export interface HdfsAcl {
  owner: string
  group: string
  sticky: boolean
  entries: HdfsAclEntry[]
  permission: number
  filePath: string
  isdir: boolean
}

/** 特殊目录（垃圾箱 / staging）快速查找结果。 */
export interface FindDirsResult {
  dirs: HdfsFile[]
  /** 已访问（list 过）的目录数 */
  visited: number
  /** 是否因达到上限而提前结束 */
  truncated: boolean
  /** 是否因达到最大深度而停止 */
  stoppedAtMaxDepth: boolean
  /** 前若干条错误信息 */
  errors: string[]
}

/** Kerberos 登录结果。 */
export interface KerberosLoginResult {
  ok: boolean
  /** 使用的 principal（`user@REALM`） */
  principal: string
  /** ccache 文件路径 */
  ccachePath: string
  /** 说明信息 */
  message: string
}

/** 一次「列出目录」的结果。 */
export interface ListResult {
  path: string
  files: HdfsFile[]
  /** 目录自身信息（不存在时为 null） */
  selfInfo: HdfsFile | null
}

/** 原生模块函数面。 */
export interface DshHdfsNative {
  nativeVersion(): string
  kerberosLoginWithConfig(config: HdfsConfig): Promise<KerberosLoginResult>
  kerberosStatusCheck(): { ok: boolean; message: string }
  listStatus(config: HdfsConfig, path: string, showContentSummary: boolean): Promise<ListResult>
  getFileStatus(config: HdfsConfig, path: string): Promise<HdfsFile>
  readFile(config: HdfsConfig, path: string, maxBytes: number): Promise<HdfsFileContent>
  writeText(config: HdfsConfig, path: string, content: string): Promise<boolean>
  uploadFile(config: HdfsConfig, localPath: string, remoteDir: string): Promise<string>
  downloadFile(config: HdfsConfig, remotePath: string, localDir: string): Promise<string>
  deletePaths(config: HdfsConfig, paths: string[], toTrash: boolean): Promise<number>
  mkdirs(config: HdfsConfig, path: string): Promise<boolean>
  createEmptyFile(config: HdfsConfig, path: string): Promise<boolean>
  renamePath(config: HdfsConfig, oldPath: string, newName: string): Promise<string>
  setPermission(config: HdfsConfig, paths: string[], permission: number, recursive: boolean): Promise<number>
  getAclStatus(config: HdfsConfig, path: string): Promise<HdfsAcl>
  modifyAclEntries(config: HdfsConfig, path: string, entries: HdfsAclEntry[], remove: boolean): Promise<boolean>
  removeAcl(config: HdfsConfig, path: string, onlyDefault: boolean): Promise<boolean>
  findSpecialDirs(
    config: HdfsConfig,
    rootPath: string,
    patterns: string[],
    maxDepth: number,
    concurrency: number,
    maxVisited: number,
    maxResults: number,
    showContentSummary: boolean,
  ): Promise<FindDirsResult>
  ping(config: HdfsConfig): Promise<boolean>
}
