/**
 * 原生模块加载烟测：验证 .node 二进制能被 Node 加载且函数面完整。
 */
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
const failures = []
const check = (label, cond) => {
  console.log(`${cond ? '✔' : '✘'} ${label}`)
  if (!cond) failures.push(label)
}

let native
try {
  native = require('../lib/native/index.cjs')
} catch (err) {
  console.error('✘ lib/native/index.cjs 加载失败:', err.message)
  console.error('  请先执行: npm run build:native')
  process.exit(1)
}

const EXPECTED = [
  'nativeVersion', 'kerberosLoginWithConfig', 'kerberosStatusCheck',
  'listStatus', 'getFileStatus', 'readFile', 'writeText',
  'uploadFile', 'downloadFile', 'deletePaths', 'mkdirs', 'createEmptyFile',
  'renamePath', 'setPermission', 'getAclStatus', 'modifyAclEntries', 'removeAcl',
  'findSpecialDirs', 'ping',
]

for (const fn of EXPECTED) {
  check(`导出 ${fn}`, typeof native[fn] === 'function')
}

check('nativeVersion 返回字符串', typeof native.nativeVersion() === 'string')
const krb = native.kerberosStatusCheck()
check('kerberosStatusCheck 返回 {ok,message}', typeof krb?.ok === 'boolean' && typeof krb?.message === 'string')
console.log(`  ↳ kerberos 状态: ok=${krb.ok}, ${krb.message}`)

if (failures.length > 0) {
  console.error(`\n${failures.length} 项检查失败`)
  process.exit(1)
}
console.log('\n全部检查通过')
