/**
 * 端到端原生栈测试：绕过宿主直接驱动 .node 模块。
 * 验证点：Kerberos 登录（keytab→TGT→ccache）→ HA nameservice 解析 → SASL 认证 → 列根目录。
 * 集群参数来自命令行参数文件（不写进代码库）。
 */
import { createRequire } from 'node:module'
import { readFileSync } from 'node:fs'

const clusterFile = process.argv[2]
if (!clusterFile) {
  console.error('用法: node scripts/e2e-native.mjs <cluster-config.json>')
  process.exit(2)
}

const require = createRequire(import.meta.url)
const native = require('../lib/native/index.cjs')
const cluster = JSON.parse(readFileSync(clusterFile, 'utf8'))

console.log('== 集群:', cluster.id, cluster.url)
console.log('== Kerberos 状态（登录前）:', JSON.stringify(native.kerberosStatusCheck()))

const config = {
  url: cluster.url,
  extraConfig: JSON.stringify(cluster.extraConfig),
  timeoutMs: 0,
}

console.log('== Kerberos 登录（keytab）…')
const login = await native.kerberosLoginWithConfig(config)
console.log('   ', JSON.stringify(login))
if (!login.ok) {
  console.error('Kerberos 登录失败，后续测试终止')
  process.exit(1)
}

console.log('== Kerberos 状态（登录后）:', JSON.stringify(native.kerberosStatusCheck()))

console.log('== ping …')
const ping = await native.ping(config)
console.log('   ', ping)

console.log('== 列根目录 …')
const list = await native.listStatus(config, '/', false)
console.log('   path:', list.path, 'files:', list.files.length)
for (const f of list.files.slice(0, 15)) {
  console.log(`    ${f.isdir ? 'd' : '-'} ${f.name}`)
}

console.log('\n✓ 原生栈端到端测试通过')
