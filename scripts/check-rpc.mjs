/**
 * 浏览器半烟测：验证 lib/client.js 符合 bundle 契约。
 */
import { readFileSync } from 'node:fs'

const src = readFileSync(new URL('../lib/client.js', import.meta.url), 'utf8')
const failures = []
const check = (label, cond) => {
  console.log(`${cond ? '✔' : '✘'} ${label}`)
  if (!cond) failures.push(label)
}

check('有 __ModuleLoader__.load 外壳', src.includes('window.__ModuleLoader__.load({ id: "dsh-hdfs", factory: (require) => {'))
check('外壳以 module.exports 收尾', src.trimEnd().endsWith('return module.exports; } });'))
check('无顶层 ESM import', !/^import /m.test(src))
check('无顶层 ESM export', !/^export /m.test(src))
check('module.exports 返回插件契约', /module\.exports = \{ name: PLUGIN_ID, inject: \['connection'\], apply \}/.test(src))
check('RPC 通道是 /api', src.includes("const RPC_CHANNEL = '/api'"))
check('RPC 前缀是 hdfs/', src.includes("const RPC_PREFIX = 'hdfs/'"))
check('有 try/catch 装载诊断', src.includes('[dsh-hdfs] client half load failed'))
check('无 .node 直载尝试（原生模块只能活在宿主）', !/\brequire\(\s*[^)]*\.node\b|process\.dlopen|\.node["']\s*\)/.test(src))

if (failures.length > 0) {
  console.error(`\n${failures.length} 项检查失败`)
  process.exit(1)
}
console.log('\n全部检查通过')
