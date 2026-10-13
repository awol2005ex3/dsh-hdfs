/**
 * 原生模块加载器（仅宿主半使用）。
 *
 * napi 构建产物在 `lib/native/`：
 *   - `index.cjs`：CJS 绑定（根 package.json 是 "type": "module"，所以必须是 .cjs）；
 *   - `index.d.ts`：官方生成的类型声明；
 *   - `dsh-hdfs-native.<triple>.node`：真正的原生二进制。
 *
 * 用 `createRequire(import.meta.url)` 相对编译产物定位：运行时本文件位于
 * `lib/native.js`，所以 `./native/index.cjs` 即 `lib/native/index.cjs`。
 */
import { createRequire } from 'node:module'
import type { DshHdfsNative } from './native-types.js'

let cached: DshHdfsNative | null = null
let cachedError: Error | null = null

/**
 * 惰性加载原生模块；进程内只加载一次。
 * 抛错时缓存错误，让后续调用拿到一致的失败信息（例如未构建 / 平台不匹配）。
 */
export function loadNative(): DshHdfsNative {
  if (cached) return cached
  if (cachedError) throw cachedError
  try {
    const require = createRequire(import.meta.url)
    cached = require('./native/index.cjs') as DshHdfsNative
    return cached
  } catch (err) {
    cachedError = new Error(
      `dsh-hdfs 原生模块加载失败（请先执行 npm run build:native）: ${err instanceof Error ? err.message : String(err)}`,
    )
    throw cachedError
  }
}

/** 原生模块是否可用（不抛错版本，供启动日志使用）。 */
export function tryNative(): DshHdfsNative | undefined {
  try {
    return loadNative()
  } catch {
    return undefined
  }
}
