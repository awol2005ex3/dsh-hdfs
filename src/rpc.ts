/**
 * dsh-hdfs 宿主半 RPC：把浏览器半的请求桥接到 Rust 原生模块。
 *
 * 通道契约（harness 0.1.5+，0.1.7 验证不变）：
 * - 0.1.5 起 `connection.rpc.handle` 对插件失效（connection fiber 不再注入 webServer），
 *   插件 RPC 的唯一正解是 `connection.fetch.register()` 注册 `/api/hdfs/<endpoint>`
 *   精确路由（exact 路由优先于 /api prefix，鉴权与体积上限由 Connection 统一施加）。
 * - 浏览器 transport POST `{ type:'client-request', rpcId, method, payload }`；
 *   本端回 `HTTP 200` + `{ type:'server-response', rpcId, result }`。
 * - result 必须是 `{ ok:true, value }` 或 `{ ok:false, error:{ code, message, details } }`；
 *   code/details 不可省略，浏览器信封校验会拒绝残缺错误。
 * - 业务错误一律 HTTP 200 + 错误分支；错误码用闭合集里的 'bad-request'。
 */
import type { HdfsSettings, HdfsCluster } from './settings.js'
import type {
  DshHdfsNative,
  HdfsAclEntry,
  HdfsConfig as NativeHdfsConfig,
} from './native-types.js'

/** 宿主 connection.fetch 的最小形状（见 harness connection/src/rpc.ts）。 */
export interface HostConnectionFetch {
  register(route: {
    path: string
    methods: readonly ('GET' | 'HEAD' | 'POST')[]
    requestBody: 'buffered' | 'streaming'
    fetch: (request: Request) => Promise<Response>
  }): () => Promise<void>
}

/** 载体中立的 RPC 结果（浏览器 transport 严格要求 code/details）。 */
export interface RpcResult {
  ok: boolean
  value?: unknown
  error?: { code: string; message: string; details: object }
}

/** 精确 Fetch 路由的 base（必须在 /api 下）。 */
export const RPC_BASE = '/api/hdfs'
/** 端点前缀（rpc.call 的 method 字段）。 */
export const RPC_PREFIX = 'hdfs/'
/** 暴露为精确路由的端点清单。 */
export const RPC_ENDPOINTS = [
  'version',
  'clusters',
  'ping',
  'list',
  'status',
  'read',
  'write',
  'mkdirs',
  'createEmpty',
  'rename',
  'delete',
  'chmod',
  'getAcl',
  'modifyAcl',
  'removeAcl',
  'findSpecialDirs',
  'kerberosLogin',
  'kerberosStatus',
  'upload',
  'download',
] as const

export type RpcEndpoint = (typeof RPC_ENDPOINTS)[number]

interface ClientRequestEnvelope {
  type?: unknown
  rpcId?: unknown
  method?: unknown
  payload?: unknown
}

/** 把任意异常折叠成合法的 RpcResult 错误分支。 */
function failure(err: unknown): RpcResult {
  const message = err instanceof Error ? err.message : String(err)
  return { ok: false, error: { code: 'bad-request', message, details: {} } }
}

function ok(value: unknown): RpcResult {
  return { ok: true, value }
}

/** 宽松字符串读取（args 校验从简：原生模块会做最终校验）。 */
function str(args: Record<string, unknown>, key: string, fallback = ''): string {
  const v = args[key]
  return typeof v === 'string' ? v : fallback
}

function num(args: Record<string, unknown>, key: string, fallback: number): number {
  const v = args[key]
  return typeof v === 'number' && Number.isFinite(v) ? v : fallback
}

function bool(args: Record<string, unknown>, key: string, fallback: boolean): boolean {
  const v = args[key]
  return typeof v === 'boolean' ? v : fallback
}

function strArray(args: Record<string, unknown>, key: string): string[] {
  const v = args[key]
  return Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string') : []
}

function aclEntries(args: Record<string, unknown>, key: string): HdfsAclEntry[] {
  const v = args[key]
  if (!Array.isArray(v)) return []
  return v
    .filter((row): row is Record<string, unknown> => typeof row === 'object' && row !== null)
    .map((row) => ({
      rtype: typeof row.rtype === 'string' ? row.rtype : 'user',
      scope: typeof row.scope === 'string' ? row.scope : 'access',
      permissions: typeof row.permissions === 'string' ? row.permissions : '',
      ...(typeof row.name === 'string' && row.name ? { name: row.name } : {}),
    }))
}

/** 端点 → 是否需要集群配置（version / clusters / kerberosStatus 不需要）。 */
const CLUSTER_FREE = new Set<string>(['version', 'clusters', 'kerberosStatus'])

/**
 * 构造 RPC 分发器。
 *
 * @param native 原生模块；null（加载失败）时所有集群端点返回可读错误
 * @param settings 现读配置快照提供者（volatile 引用值随时在变，每次请求现读）
 */
export function createRpcDispatcher(
  native: DshHdfsNative | null,
  settings: () => HdfsSettings & { clusterError?: string },
): (endpoint: string, args: Record<string, unknown>) => Promise<RpcResult> {
  /** 把集群配置映射成原生模块的 HdfsConfig。 */
  const nativeConfig = (cluster: HdfsCluster): NativeHdfsConfig => ({
    url: cluster.url,
    extraConfig: cluster.extraConfig ?? '{}',
    timeoutMs: 0,
  })

  /** 按 id（或默认集群）解析集群；找不到时给出含候选清单的可读错误。 */
  function resolveCluster(args: Record<string, unknown>): HdfsCluster {
    const snap = settings()
    if (snap.clusters.length === 0) {
      throw new Error(
        snap.clusterError
          ? `未配置可用集群：${snap.clusterError}（设置 → 插件 → HDFS）`
          : '未配置 HDFS 集群（设置 → 插件 → HDFS，clusters 为 JSON 数组）',
      )
    }
    const wanted = str(args, 'cluster') || snap.defaultCluster
    const found = snap.clusters.find((c) => c.id === wanted)
    if (found) return found
    throw new Error(
      `集群 "${wanted}" 不存在。可用: ${snap.clusters.map((c) => c.id).join(', ')}`,
    )
  }

  return async function dispatch(endpoint, args): Promise<RpcResult> {
    if (CLUSTER_FREE.has(endpoint)) {
      switch (endpoint) {
        case 'version':
          return ok({ native: native ? native.nativeVersion() : 'unavailable' })
        case 'clusters': {
          const snap = settings()
          return ok({
            clusters: snap.clusters.map((c: HdfsCluster) => ({ id: c.id, url: c.url })),
            defaultCluster: snap.defaultCluster,
            clusterError: snap.clusterError,
          })
        }
        case 'kerberosStatus':
          return ok(native ? native.kerberosStatusCheck() : { ok: false, message: 'native module not loaded' })
        default:
          return failure(new Error(`unknown endpoint ${endpoint}`))
      }
    }

    // 其余端点都需要集群 + 原生模块
    if (!native) {
      return failure(new Error('dsh-hdfs 原生模块未加载（宿主启动日志应有警告）；请先 npm run build:native 并重启'))
    }
    let cluster: HdfsCluster
    try {
      cluster = resolveCluster(args)
    } catch (err) {
      return failure(err)
    }
    const config = nativeConfig(cluster)

    try {
      switch (endpoint) {
        case 'ping':
          return ok({ cluster: cluster.id, ok: await native.ping(config) })

        case 'list': {
          const path = str(args, 'path', '/')
          const summary = bool(args, 'summary', false)
          return ok(await native.listStatus(config, path, summary))
        }

        case 'status':
          return ok(await native.getFileStatus(config, str(args, 'path', '/')))

        case 'read': {
          const snap = settings()
          const max = num(args, 'maxBytes', snap.readLimitBytes)
          return ok(await native.readFile(config, str(args, 'path'), Math.max(0, Math.floor(max))))
        }

        case 'write':
          return ok({ ok: await native.writeText(config, str(args, 'path'), str(args, 'content')) })

        case 'mkdirs':
          return ok({ ok: await native.mkdirs(config, str(args, 'path')) })

        case 'createEmpty':
          return ok({ ok: await native.createEmptyFile(config, str(args, 'path')) })

        case 'rename':
          return ok({
            path: await native.renamePath(config, str(args, 'oldPath'), str(args, 'newName')),
          })

        case 'delete': {
          const paths = strArray(args, 'paths')
          if (paths.length === 0) throw new Error('paths 不能为空')
          const trash = bool(args, 'trash', true)
          return ok({ removed: await native.deletePaths(config, paths, trash) })
        }

        case 'chmod': {
          const paths = strArray(args, 'paths')
          if (paths.length === 0) throw new Error('paths 不能为空')
          const permission = Math.floor(num(args, 'permission', 0o755))
          if (permission < 0 || permission > 0o1777) throw new Error(`非法权限: ${permission}`)
          return ok({
            changed: await native.setPermission(config, paths, permission, bool(args, 'recursive', false)),
          })
        }

        case 'getAcl':
          return ok(await native.getAclStatus(config, str(args, 'path', '/')))

        case 'modifyAcl': {
          const entries = aclEntries(args, 'entries')
          if (entries.length === 0) throw new Error('entries 不能为空')
          return ok({
            ok: await native.modifyAclEntries(config, str(args, 'path'), entries, bool(args, 'remove', false)),
          })
        }

        case 'removeAcl':
          return ok({
            ok: await native.removeAcl(config, str(args, 'path'), bool(args, 'onlyDefault', false)),
          })

        case 'findSpecialDirs': {
          const snap = settings()
          const patterns = strArray(args, 'patterns')
          if (patterns.length === 0) throw new Error('patterns 不能为空')
          return ok(
            await native.findSpecialDirs(
              config,
              str(args, 'rootPath', '/'),
              patterns,
              Math.floor(num(args, 'maxDepth', snap.findMaxDepth)),
              Math.floor(num(args, 'concurrency', snap.findConcurrency)),
              Math.floor(num(args, 'maxVisited', 20000)),
              Math.floor(num(args, 'maxResults', 500)),
              bool(args, 'summary', false),
            ),
          )
        }

        case 'kerberosLogin':
          return ok(await native.kerberosLoginWithConfig(config))

        case 'upload': {
          const local = str(args, 'localPath')
          const remoteDir = str(args, 'remoteDir', '/')
          if (!local) throw new Error('localPath 不能为空')
          return ok({ path: await native.uploadFile(config, local, remoteDir) })
        }

        case 'download': {
          const remote = str(args, 'remotePath')
          const localDir = str(args, 'localDir')
          if (!remote) throw new Error('remotePath 不能为空')
          if (!localDir) throw new Error('localDir 不能为空')
          return ok({ path: await native.downloadFile(config, remote, localDir) })
        }

        default:
          return failure(new Error(`unknown endpoint ${endpoint}`))
      }
    } catch (err) {
      return failure(err)
    }
  }
}

/**
 * 为每个端点注册精确 Fetch 路由，并回填 0.1.5 信封。
 *
 * @returns 解除全部路由的 disposer（幂等）
 */
export function registerRpcRoutes(
  connection: unknown,
  dispatch: (endpoint: string, args: Record<string, unknown>) => Promise<RpcResult>,
  log: (message: string) => void,
): () => void {
  const registry = (connection as { fetch?: HostConnectionFetch } | undefined)?.fetch
  if (!registry || typeof registry.register !== 'function') {
    log('connection.fetch registry unavailable; Web client RPC disabled')
    return () => {}
  }

  /** 回填 server-response 信封（业务错误也保持 HTTP 200）。 */
  const answer = (endpoint: string) => async (request: Request): Promise<Response> => {
    let rpcId = ''
    // 信封解析/校验失败 → 400（浏览器 transport 能感知载体层错误）
    let body: ClientRequestEnvelope
    try {
      body = (await request.json()) as ClientRequestEnvelope
    } catch {
      return new Response('malformed rpc envelope', { status: 400 })
    }
    if (body?.type !== 'client-request' || typeof body.rpcId !== 'string') {
      return new Response('malformed rpc envelope', { status: 400 })
    }
    rpcId = body.rpcId
    try {
      const payload = (body.payload ?? {}) as { args?: Record<string, unknown> }
      const args = payload.args ?? {}
      const result = await dispatch(endpoint, args)
      return new Response(JSON.stringify({ type: 'server-response', rpcId, result }), {
        headers: { 'content-type': 'application/json' },
      })
    } catch (err) {
      // dispatch 内部已把业务错误折叠进 RpcResult；到这里说明框架级异常。
      void rpcId
      return new Response(`hdfs rpc failed: ${err instanceof Error ? err.message : String(err)}`, {
        status: 500,
      })
    }
  }

  const disposers: Array<() => unknown> = []
  for (const endpoint of RPC_ENDPOINTS) {
    try {
      const remove = registry.register({
        path: `${RPC_BASE}/${endpoint}`,
        methods: ['POST'],
        requestBody: 'buffered',
        fetch: answer(endpoint),
      })
      disposers.push(remove)
    } catch (err) {
      log(`failed to register route ${RPC_BASE}/${endpoint}: ${String(err)}`)
    }
  }

  return () => {
    for (const dispose of disposers) {
      try {
        void dispose()
      } catch {
        // 单个路由解除失败不阻断其余清理
      }
    }
  }
}
