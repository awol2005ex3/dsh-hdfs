// @dsh-version 0.1.7-rc.2
/**
 * dsh-hdfs · 宿主半（Node / Cordis 插件）。
 *
 * 职责：
 * 1. 加载 rust/ 构建出的 napi 原生模块（lib/native/index.cjs → .node）；
 * 2. 经 `connection.fetch.register()` 注册 `/api/hdfs/<endpoint>` 精确路由
 *    （0.1.5 起 rpc.handle 对插件失效，这是插件 RPC 的唯一正解）；
 * 3. 注册 `hdfs` agent 工具（复用 RPC 分发器，浏览器半与工具走同一条校验路径）。
 *
 * 浏览器半（lib/client.js）通过 `connection.rpc.call('/api', 'hdfs/<endpoint>', …)`
 * 调用这里注册的路由 —— .node 模块永远不能进浏览器，RPC 桥是唯一通路。
 */
import type { Context } from '@deepseek-ai/cordis'

import { HdfsConfigSchema, readHdfsSettings, type HdfsConfig } from './settings.js'
import { loadNative, tryNative } from './native.js'
import { createRpcDispatcher, registerRpcRoutes } from './rpc.js'
import { createHdfsTool } from './tool.js'

/** Plugin id — must be unique within the composed plugin tree. */
export const name = 'dsh-hdfs'

/** Declare the host services this plugin needs; the loader waits for them. */
export const inject = ['connection', 'tools'] as const

/**
 * 插件配置 schema（dsh ≥ 0.1.7 的配置载体）。
 * volatile 字段改动不重挂插件：loader 原地更新引用并广播 loader/volatile-update；
 * 本插件所有 RPC 请求都现读配置（readHdfsSettings），无需监听也能生效。
 */
export const Config = HdfsConfigSchema

/**
 * 配置命名空间 = profile 条目 id（本插件的 bundle patch 用 `id: hdfs`）。
 */
function settingsNamespace(ctx: Context): string {
  const fiber = (ctx as unknown as { fiber?: { entry?: { options?: { id?: string } } } }).fiber
  return fiber?.entry?.options?.id ?? 'hdfs'
}

export function apply(ctx: Context, config: HdfsConfig): void {
  const logger = ctx.logger('hdfs')

  // ── 原生模块 ──────────────────────────────────────────────────────────────
  // 加载失败不炸插件：RPC 与工具返回可读错误，浏览器半照常工作（显示错误）。
  const native = tryNative()
  if (native) {
    logger.info('dsh-hdfs: native module loaded, version %s', native.nativeVersion())
  } else {
    logger.warn('dsh-hdfs: native module unavailable; run `npm run build:native`. All cluster operations will fail.')
  }

  // volatile 配置现读快照器：每次请求现取，引用不变、值在变。
  const settings = () => readHdfsSettings(config)

  // ── RPC 分发器（浏览器半 + agent 工具共用） ───────────────────────────────
  // native 为 null 时 dispatcher 对所有集群端点返回可读错误。
  const dispatch = createRpcDispatcher(native ?? null, settings)

  // ── 注册 Fetch 精确路由（ctx.effect 保证 HMR/卸载可逆） ───────────────────
  const connection = ctx.get('connection')
  ctx.effect(() => {
    const dispose = registerRpcRoutes(connection, dispatch, (m) => logger.warn('dsh-hdfs: %s', m))
    logger.info('dsh-hdfs: /api/hdfs/* routes registered (ns=%s)', settingsNamespace(ctx))
    return dispose
  }, 'hdfs: rpc routes')

  // ── 注册 agent 工具 ──────────────────────────────────────────────────────
  ctx.effect(() => {
    // tools 服务由 loader 提供；ctx.get 兼容测试替身，属性访问只适用于真实 cordis。
    const tools = (ctx as unknown as { get?: (n: string) => unknown }).get?.('tools')
      ?? (ctx as unknown as { tools?: { register: unknown } }).tools
    const registry = tools as { register: (t: unknown) => unknown } | undefined
    const tool = createHdfsTool(async (endpoint, args) => {
      const result = await dispatch(endpoint, args)
      if (result.ok) return result.value
      throw new Error(result.error?.message ?? 'hdfs rpc failed')
    })
    if (!registry || typeof registry.register !== 'function') {
      // 无工具注册表时退化为空 effect（仍返回合法 disposer）。
      logger.warn('dsh-hdfs: tools registry unavailable; agent tool not registered')
      return () => {}
    }
    logger.info('dsh-hdfs: hdfs agent tool registered')
    // register 回 disposer（cordis collect 风格）；收窄类型以满足 ctx.effect 的 Effect 签名。
    const dispose = registry.register(tool) as () => void
    return () => { dispose() }
  }, 'hdfs: agent tool')
}
