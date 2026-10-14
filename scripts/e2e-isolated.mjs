/**
 * 隔离验证新版 .node：从临时目录加载（宿主锁着的旧版不受影响）。
 * 复现宿主进程的环境条件：KRB5CCNAME=API:krb5cc（非 FILE: 型）。
 */
import { createRequire } from 'node:module'
import { readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

process.env.KRB5CCNAME = 'API:krb5cc' // 复现宿主环境

const clusterFile = process.argv[2]
const require = createRequire(import.meta.url)
const native = require(join(tmpdir(), 'dsh-hdfs-native-test', 'index.cjs'))
const cluster = JSON.parse(readFileSync(clusterFile, 'utf8'))

console.log('KRB5CCNAME =', JSON.stringify(process.env.KRB5CCNAME))
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
  console.error('✘ Kerberos 登录失败')
  process.exit(1)
}

console.log('== Kerberos 状态（登录后）:', JSON.stringify(native.kerberosStatusCheck()))

console.log('== ping …')
console.log('   ', await native.ping(config))

console.log('== 列根目录 …')
const list = await native.listStatus(config, '/', false)
console.log('   path:', list.path, 'files:', list.files.length)
for (const f of list.files.slice(0, 15)) console.log(`    ${f.isdir ? 'd' : '-'} ${f.name}`)

console.log('\n✓ 隔离验证通过（API:krb5cc 环境下 ccache 落点与 SASL 均正常）')
