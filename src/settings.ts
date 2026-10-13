import z from '@deepseek-ai/schemastery'

/**
 * 只依赖 `get()` 的结构类型。
 *
 * dsh ≥ 0.1.7 里插件的 Config 字段由 schemastery 的 `.volatile()` 解析成 cosmokit 的
 * `Volatile<T>` **稳定引用**（改动时 loader 原地更新引用、不重挂插件）。这里只声明
 * 用得到的 `get()`，既不把插件绑死在某个 cosmokit 版本上，也让旧版 dsh（字段是普通值）
 * 与测试替身同样通过类型检查。
 */
export interface VolatileRef<T> {
  get(): T
}

/** 字段可能是 volatile 引用（dsh ≥ 0.1.7）或普通值（旧版 dsh / 测试替身）。 */
export type MaybeVolatile<T> = T | VolatileRef<T>

/** 一个 HDFS 集群连接的**普通值**配置（与原生模块的 `HdfsConfig` 对应）。 */
export interface HdfsCluster {
  /** 集群显示名 / 选择 id（面板下拉与工具参数都用它） */
  id: string
  /** `hdfs://nameservice1` 或 `hdfs://host:8020` */
  url: string
  /** Hadoop 扩展配置（JSON 对象字符串），含 HA / Kerberos 参数 */
  extraConfig: string
}

/** 宿主半与工具使用的**普通值**配置快照。 */
export interface HdfsSettings {
  clusters: HdfsCluster[]
  defaultCluster: string
  /** 读文件预览的字节上限（浏览器与工具共用），默认 2 MiB */
  readLimitBytes: number
  /** 查找特殊目录的默认并发 */
  findConcurrency: number
  /** 查找特殊目录的默认最大深度 */
  findMaxDepth: number
}

/**
 * `Config` schema 的解析结果：每个字段都可能是 volatile 引用。
 *
 * ⚠ 不要在这里做业务判断——一律经 {@link readHdfsSettings} 解包成普通值。
 */
export interface HdfsConfig {
  clusters?: MaybeVolatile<string>
  defaultCluster?: MaybeVolatile<string>
  readLimitBytes?: MaybeVolatile<number>
  findConcurrency?: MaybeVolatile<number>
  findMaxDepth?: MaybeVolatile<number>
}

/** 与 Schema 默认值保持一致的常量兜底。 */
const DEFAULTS = {
  clusters: '[]',
  defaultCluster: '',
  readLimitBytes: 2 * 1024 * 1024,
  findConcurrency: 8,
  findMaxDepth: 6,
}

/**
 * HDFS 插件的 **Config schema**（dsh ≥ 0.1.7 的配置载体）。
 *
 * 集群列表存成 JSON 字符串（`[{id,name,url,extraConfig}]`）——schemastery 的表单
 * 对「对象数组」的编辑体验差，JSON 文本域 + 面板里的「格式校验」按钮更实用。
 * 只有标记 `.volatile()` 的字段会进入「设置 → 插件」表单并被允许热改。
 */
export const HdfsConfigSchema = z.object({
  clusters: z.string().default(DEFAULTS.clusters).volatile().description(
    'HDFS 集群列表（JSON 数组）：[{"id":"prod","url":"hdfs://nn1:8020","extraConfig":"{}"}]。' +
    'extraConfig 可填 HA 参数（dfs.nameservices 等）与 Kerberos 参数' +
    '（dfs.namenode.kerberos.principal / dfs.namenode.keytab.file）',
  ),
  defaultCluster: z.string().default(DEFAULTS.defaultCluster).volatile().description(
    '默认集群 id（面板与工具省略 cluster 参数时使用）。留空取列表第一个',
  ),
  readLimitBytes: z.number().default(DEFAULTS.readLimitBytes).volatile().description(
    '读文件预览的默认字节上限（0 = 不限制，谨慎）',
  ),
  findConcurrency: z.number().default(DEFAULTS.findConcurrency).volatile().description(
    '特殊目录查找的默认并发数（1-32）',
  ),
  findMaxDepth: z.number().default(DEFAULTS.findMaxDepth).volatile().description(
    '特殊目录查找的默认最大深度（1-12）',
  ),
})

/** 解包 volatile 引用（dsh ≥ 0.1.7）或普通值（旧版 dsh / 测试替身）。 */
function unwrap<T>(value: MaybeVolatile<T> | undefined | null): T | undefined {
  if (value === undefined || value === null) return undefined
  const ref = value as VolatileRef<T>
  if (typeof ref.get === 'function') {
    try {
      return ref.get()
    } catch {
      // 引用被替换/尚未提交时不致命：退回默认值。
      return undefined
    }
  }
  return value as T
}

/** 解析集群 JSON 列表；坏条目跳过并返回错误消息（不抛，让面板能显示）。 */
export function parseClusters(raw: string | undefined): { clusters: HdfsCluster[]; error?: string } {
  const text = (raw ?? '').trim()
  if (!text) return { clusters: [] }
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch (err) {
    return { clusters: [], error: `clusters JSON 解析失败: ${err instanceof Error ? err.message : String(err)}` }
  }
  if (!Array.isArray(parsed)) return { clusters: [], error: 'clusters 必须是 JSON 数组' }

  const clusters: HdfsCluster[] = []
  const seen = new Set<string>()
  for (const [index, item] of parsed.entries()) {
    const row = item as Partial<HdfsCluster>
    const url = typeof row?.url === 'string' ? row.url.trim() : ''
    if (!url) continue
    let id = typeof row?.id === 'string' && row.id.trim() ? row.id.trim() : ''
    if (!id) {
      // 无 id 时从 URL 推导：hdfs://nn1:8020 → nn1；重复时补序号
      id = url.replace(/^hdfs:\/\//, '').replace(/:\d+$/, '').replace(/[^\w.-]+/g, '-') || `cluster${index + 1}`
    }
    let unique = id
    for (let n = 2; seen.has(unique); n += 1) unique = `${id}-${n}`
    seen.add(unique)
    clusters.push({
      id: unique,
      url,
      extraConfig: typeof row?.extraConfig === 'string' ? row.extraConfig : '{}',
    })
  }
  return { clusters }
}

/**
 * 取一份**普通值**配置快照。
 *
 * 每次请求处理前都调用它（而不是缓存 apply 时的配置）：volatile 字段的值随时可能被
 * 设置页改写，引用本身不变、值在变，所以必须现读现用。
 */
export function readHdfsSettings(config?: HdfsConfig | null): HdfsSettings & { clusterError?: string } {
  const source: HdfsConfig = config ?? {}
  const { clusters, error } = parseClusters(unwrap(source.clusters))
  const defaultCluster = unwrap(source.defaultCluster)?.trim() ?? ''
  const limit = unwrap(source.readLimitBytes)
  const concurrency = unwrap(source.findConcurrency)
  const depth = unwrap(source.findMaxDepth)

  return {
    clusters,
    clusterError: error,
    defaultCluster: defaultCluster || clusters[0]?.id || '',
    readLimitBytes: typeof limit === 'number' && Number.isFinite(limit) && limit >= 0 ? limit : DEFAULTS.readLimitBytes,
    findConcurrency: typeof concurrency === 'number' && Number.isFinite(concurrency) ? Math.min(32, Math.max(1, Math.floor(concurrency))) : DEFAULTS.findConcurrency,
    findMaxDepth: typeof depth === 'number' && Number.isFinite(depth) ? Math.min(12, Math.max(1, Math.floor(depth))) : DEFAULTS.findMaxDepth,
  }
}
