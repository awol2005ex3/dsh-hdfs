/**
 * 宿主半冒烟检查：模拟 cordis 环境加载 lib/index.js，
 * 验证四大导出、原生模块加载、工具注册与 RPC 路由注册。
 */
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'

const require = createRequire(import.meta.url)
const pluginUrl = new URL('../lib/index.js', import.meta.url)

const failures = []
const check = (label, cond) => {
  console.log(`${cond ? '✔' : '✘'} ${label}`)
  if (!cond) failures.push(label)
}

// ── 加载插件主入口 ──
let plugin
try {
  plugin = await import(pluginUrl.href)
} catch (err) {
  console.error('✘ lib/index.js 加载失败:', err)
  process.exit(1)
}

check('name 导出 = dsh-hdfs', plugin.name === 'dsh-hdfs')
check('inject 含 connection + tools', Array.isArray(plugin.inject)
  && plugin.inject.includes('connection') && plugin.inject.includes('tools'))
check('Config schema 存在（schemastery schema 为函数形态）', typeof plugin.Config === 'object' || typeof plugin.Config === 'function')
check('apply 是函数', typeof plugin.apply === 'function')

// ── 模拟宿主环境 ──
const registeredRoutes = []
const registeredTools = []
const disposers = []

const fakeConnection = {
  fetch: {
    register(route) {
      registeredRoutes.push(route)
      const remove = async () => {
        const i = registeredRoutes.indexOf(route)
        if (i >= 0) registeredRoutes.splice(i, 1)
      }
      disposers.push(remove)
      return remove
    },
  },
}

const fakeCtx = {
  // cordis 的 logger 是可调用访问器：ctx.logger('name').info(...)
  logger: (name) => ({
    info: () => {},
    warn: (msg, ...args) => console.log(`  [warn:${name}]`, msg, ...args),
    error: (msg, ...args) => console.log(`  [error:${name}]`, msg, ...args),
  }),
  get: (name) => (name === 'connection' ? fakeConnection : name === 'tools' ? fakeTools : undefined),
  effect: (fn, label) => {
    const dispose = fn()
    disposers.push(() => dispose?.())
    return label
  },
}

const fakeTools = {
  register(tool) {
    registeredTools.push(tool)
    return () => {
      const i = registeredTools.indexOf(tool)
      if (i >= 0) registeredTools.splice(i, 1)
    }
  },
}

// 带两个集群的测试配置（volatile 引用形式，get() 现读）
const clusters = [
  { id: 'test1', url: 'hdfs://localhost:9000', extraConfig: '{}' },
  { id: 'bad', url: 'hdfs://nonexistent.invalid:8020', extraConfig: '{}' },
]
const config = {
  clusters: { get: () => JSON.stringify(clusters) },
  defaultCluster: { get: () => 'test1' },
  readLimitBytes: { get: () => 65536 },
}

// ── apply ──
try {
  plugin.apply(fakeCtx, config)
} catch (err) {
  console.error('✘ apply 抛错:', err)
  process.exit(1)
}

check('注册了 20 条 /api/hdfs/* 路由', registeredRoutes.length === 20)
check('路由全部在 /api 下', registeredRoutes.every((r) => r.path.startsWith('/api/hdfs/')))
check('路由全部 POST + buffered', registeredRoutes.every((r) => r.methods.length === 1 && r.methods[0] === 'POST' && r.requestBody === 'buffered'))
check('注册了 1 个 hdfs 工具', registeredTools.length === 1 && registeredTools[0].name === 'hdfs')

// ── 通过 Fetch 路由调用 RPC（绕过 HTTP，直接调 fetch handler） ──
const route = (path) => registeredRoutes.find((r) => r.path === path)
const callRoute = async (path, args) => {
  const r = route(path)
  if (!r) throw new Error(`route ${path} not found`)
  const request = new Request(`http://localhost${path}`, {
    method: 'POST',
    body: JSON.stringify({ type: 'client-request', rpcId: 'test-1', method: 'x', payload: { args } }),
  })
  const response = await r.fetch(request)
  if (response.status !== 200) throw new Error(`HTTP ${response.status}: ${await response.text()}`)
  const body = await response.json()
  if (body.type !== 'server-response' || body.rpcId !== 'test-1') throw new Error(`bad envelope: ${JSON.stringify(body)}`)
  return body.result
}

// version / clusters / kerberosStatus（无需真实集群）
const version = await callRoute('/api/hdfs/version', {})
check('version 返回原生版本', version.ok && typeof version.value?.native === 'string')

const clusterList = await callRoute('/api/hdfs/clusters', {})
check('clusters 返回 2 个集群', clusterList.ok && clusterList.value?.clusters?.length === 2)
check('clusters.defaultCluster = test1', clusterList.value?.defaultCluster === 'test1')

const krb = await callRoute('/api/hdfs/kerberosStatus', {})
check('kerberosStatus 返回 {ok,message}', krb.ok && typeof krb.value?.ok === 'boolean' && typeof krb.value?.message === 'string')

// 错误路径：未知集群
const badCluster = await callRoute('/api/hdfs/list', { cluster: 'nope', path: '/' })
check('未知集群 → ok:false + code/message/details', !badCluster.ok && badCluster.error?.code && badCluster.error?.message && 'details' in badCluster.error)

// 错误路径：native 缺席时（这里 native 已加载，跳过）——连不存在的集群验证错误信封
const unreachable = await callRoute('/api/hdfs/ping', { cluster: 'bad' })
check('不可达集群 → ok:false + 可读错误', !unreachable.ok && typeof unreachable.error?.message === 'string')

// 工具 execute 烟测
const tool = registeredTools[0]
const exec = { signal: undefined }
const toolOk = await tool.execute({ endpoint: 'clusters', args: {} }, exec)
check('工具 clusters 调用成功', toolOk.ok === true && toolOk.result.includes('test1'))

const toolBad = await tool.execute({ endpoint: 'list', args: { cluster: 'nope' } }, exec)
check('工具错误 → ok:false + 错误消息', toolBad.ok === false && /集群 "nope" 不存在/.test(toolBad.result))

// 信封畸形：HTTP 400
const malformed = route('/api/hdfs/version')
const badResp = await malformed.fetch(new Request('http://localhost/api/hdfs/version', {
  method: 'POST',
  body: 'not-json',
}))
check('畸形信封 → HTTP 400', badResp.status === 400)

// ── disposer 幂等 ──
for (const d of disposers) await d()
check('disposer 解除后路由清空', registeredRoutes.length === 0)

// ── 结论 ──
if (failures.length > 0) {
  console.error(`\n${failures.length} 项检查失败`)
  process.exit(1)
}
console.log('\n全部检查通过')
