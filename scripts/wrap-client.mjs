import { readFileSync, writeFileSync } from 'node:fs'

const id = 'dsh-hdfs'
const path = new URL('../lib/client.js', import.meta.url)

let body = readFileSync(path, 'utf8')
body = body.replace(/\nexport \{\};?\s*$/, '\n')

const banner = `window.__ModuleLoader__.load({ id: ${JSON.stringify(id)}, factory: (require) => {`
// 装载期诊断：浏览器只会显示「N entry did not activate / dsh-hdfs: failed」，
// 真实原因只在控制台。这里包一层 try/catch，把错误与栈带标签送到 console.error。
const intro = [
  'var module = { exports: {} }; var exports = module.exports;',
  'try {',
].join('\n')
const footer = [
  '} catch (error) {',
  "  console.error('[dsh-hdfs] client half load failed:', (error && error.stack) || error);",
  '  throw error;',
  '}',
  'return module.exports; } });',
].join('\n')

writeFileSync(path, `${banner}\n${intro}\n${body}${footer}\n`)
console.log(`wrap-client: ${path.pathname} wrapped as ${id}`)
