/**
 * dsh-hdfs · 浏览器端 bundle 模块体。
 *
 * 构建契约同 dsh-wecom/src/client.ts：本文件经 tsc 编译后，由
 * scripts/wrap-client.mjs 包上闭包工厂外壳（无 ESM 语法，module.exports 返回
 * { name, inject, apply }）。
 *
 * 功能：
 * 1. HDFS 浏览面板：集群下拉 + 目录树表格 + 文件预览 + Kerberos 登录/状态；
 * 2. 集群配置编辑（clusters JSON 文本域 + 校验），读写走官方 configForms，
 *    退路是插件宿主 RPC；
 * 3. 挂载策略逐级降级：settings.plugins.tab（0.1.7）→ settings.plugin.item →
 *    settings.section → 侧边栏/浮动启动器。
 *
 * ⚠ RPC 通道固定 /api：宿主端是 connection.fetch.register() 注册的精确路由
 * （0.1.5 起 rpc.handle 对插件失效），只有 /api 前缀才会被路由表命中。
 * ⚠ .node 原生模块只能活在宿主进程里，浏览器半永远经由 RPC 桥访问 HDFS。
 */

const PLUGIN_ID = 'dsh-hdfs'
const SETTINGS_NS = 'hdfs'
const RPC_CHANNEL = '/api'
const RPC_PREFIX = 'hdfs/'

declare const module: { exports: unknown }
declare function require(id: string): unknown

const win = window as unknown as { __dshHdfsMounted?: boolean }
const doc = document

/* ── 工具函数 ─────────────────────────────────────────────────────────── */

type ElProps = { style?: string; [key: string]: unknown }

function el<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  props?: ElProps,
  children?: (Node | string)[],
): HTMLElementTagNameMap[K] {
  const node = doc.createElement(tag)
  if (props) {
    for (const [key, value] of Object.entries(props)) {
      if (key === 'style') node.setAttribute('style', value as string)
      else (node as unknown as Record<string, unknown>)[key] = value
    }
  }
  if (children) {
    for (const c of children) node.append(typeof c === 'string' ? doc.createTextNode(c) : c)
  }
  return node
}

function fmtBytes(n: number): string {
  if (!Number.isFinite(n)) return '-'
  if (n < 1024) return `${n} B`
  const units = ['KB', 'MB', 'GB', 'TB', 'PB']
  let v = n
  let i = -1
  do { v /= 1024; i += 1 } while (v >= 1024 && i < units.length - 1)
  return `${v.toFixed(v >= 100 ? 0 : 1)} ${units[i]}`
}

function fmtTime(ms: number): string {
  if (!ms) return '-'
  try { return new Date(ms).toLocaleString() } catch { return String(ms) }
}

/** 八进制权限位 → rwxr-xr-x 形式。 */
function fmtPerm(bits: number): string {
  const map = ['---', '--x', '-w-', '-wx', 'r--', 'r-x', 'rw-', 'rwx']
  const str = (bits & 0o777).toString(8).padStart(3, '0')
  return str.split('').map((d) => map[Number(d)] ?? '---').join('')
}

/** 安全地取一个服务（cordis 4 严格 inject：属性访问会抛，get 不受限制）。 */
function service(ctx: any, name: string): any {
  try {
    if (typeof ctx?.get === 'function') return ctx.get(name)
    return ctx?.[name]
  } catch {
    return undefined
  }
}

/* ── RPC ──────────────────────────────────────────────────────────────── */

interface RpcResponse {
  ok: boolean
  value?: unknown
  error?: { code: string; message: string; details?: unknown }
}

interface HdfsFileRow {
  name: string
  path: string
  owner: string
  group: string
  isdir: boolean
  permission: number
  modificationTime: number
  length: number
  fileCount?: number | null
  spaceConsumed?: number | null
}

interface ListResult {
  path: string
  files: HdfsFileRow[]
  selfInfo?: HdfsFileRow | null
}

interface ClusterInfo {
  clusters: Array<{ id: string; url: string }>
  defaultCluster: string
  clusterError?: string
}

async function callRpc(conn: any, endpoint: string, args?: Record<string, unknown>): Promise<unknown> {
  const res = (await conn.rpc.call(RPC_CHANNEL, `${RPC_PREFIX}${endpoint}`, args ? { args } : {})) as RpcResponse
  if (!res || res.ok !== true) {
    throw new Error(res?.error?.message ?? 'rpc error')
  }
  return res.value
}

/* ── 配置读写通道 ──────────────────────────────────────────────────────── */

interface SettingsChannel {
  read(): Promise<{ clusters?: string; defaultCluster?: string }>
  write(patch: Record<string, unknown>): Promise<void>
}

/** 退路通道：宿主 /api/hdfs/clusters 只读快照（宿主不落盘，写操作不可用）。 */
function rpcSettingsChannel(conn: any): SettingsChannel | undefined {
  if (!conn || !conn.rpc || typeof conn.rpc.call !== 'function') return undefined
  return {
    read: async () => {
      const value = (await callRpc(conn, 'clusters', {})) as ClusterInfo
      // host 端只回解析后的集群清单；编辑器需要原始 JSON —— 由面板自行序列化
      return { clusters: JSON.stringify(value.clusters ?? [], null, 2), defaultCluster: value.defaultCluster ?? '' }
    },
    write: async () => {
      throw new Error('当前 dsh 未提供 settings 表单服务，配置只能在宿主 profile 的 cordis.patch.yml 里改')
    },
  }
}

/** 首选通道：dsh 官方 configForms（设置页同款，volatile 字段落盘 + 热更新）。 */
function configFormsChannel(ctx: any): SettingsChannel | undefined {
  const forms = service(ctx, 'configForms')
  if (!forms || typeof forms.get !== 'function') return undefined

  /**
   * 探测命名空间是否已进入镜像。
   *
   * 不缓存结果：镜像异步拉取，插件启动时多半还没就绪（设置页首次 describe 后才填
   * view.namespaces）。wecom 的教训——首次探测失败即永久降级会让面板永远失去写能力。
   */
  function ensure(): boolean {
    try {
      const mirror = typeof forms.describe === 'function' ? forms.describe() : undefined
      if (!mirror || typeof mirror.getSnapshot !== 'function') return false
      return mirror.getSnapshot()?.view?.namespaces?.some((row: any) => row?.ns === SETTINGS_NS) === true
    } catch {
      return false
    }
  }

  if (!ensure()) return undefined

  const form = (): any => forms.get(SETTINGS_NS)
  async function waitReady(): Promise<any> {
    const snap = form().getSnapshot()
    if (snap?.status === 'ready') return snap
    return new Promise((resolve, reject) => {
      let done = false
      const timer = setTimeout(() => {
        if (done) return
        done = true
        off?.()
        reject(new Error('读取配置超时（宿主配置镜像未就绪）'))
      }, 15000)
      const off = typeof form().subscribe === 'function' ? form().subscribe(check) : undefined
      function check(): void {
        if (done) return
        const s = form().getSnapshot()
        if (s?.status === 'ready') {
          done = true
          clearTimeout(timer)
          off?.()
          resolve(s)
        } else if (s?.status === 'unavailable') {
          done = true
          clearTimeout(timer)
          off?.()
          reject(new Error('配置服务不可用'))
        }
      }
    })
  }

  return {
    read: async () => {
      const snap = await waitReady()
      const value = snap.value ?? {}
      // value.clusters 可能已是解析后的数组或原始字符串，统一回原始字符串
      let clusters = value.clusters
      if (Array.isArray(clusters)) clusters = JSON.stringify(clusters, null, 2)
      return { clusters: String(clusters ?? '[]'), defaultCluster: String(value.defaultCluster ?? '') }
    },
    write: async (patch) => {
      const snap = await waitReady()
      const ops: Array<Record<string, unknown>> = []
      for (const [key, value] of Object.entries(patch)) {
        ops.push({ op: 'set', path: [key], value })
      }
      await form().mutate(ops, snap.revision)
    },
  }
}

/**
 * 运行时择优建通道：**每次读写都重新探测** `configForms`。
 *
 * 不缓存探测结果——设置页的配置镜像要等宿主首次 describe 回来才填 view.namespaces，
 * 插件启动时的探测失败不代表以后也不可用。探测代价极低（一次 map 查找）。
 */
function createChannel(ctx: any, conn: any): SettingsChannel {
  const resolve = (): SettingsChannel | undefined =>
    configFormsChannel(ctx) ?? rpcSettingsChannel(conn)

  const required = (): SettingsChannel => {
    const channel = resolve()
    if (!channel) throw new Error('没有可用的配置读写通道（configForms / RPC 均不可用）')
    return channel
  }

  return {
    read: () => required().read(),
    write: (patch) => required().write(patch),
  }
}

/* ── 面板 ─────────────────────────────────────────────────────────────── */

const PANEL_CSS =
  'color:var(--fg,#1f2328);font-size:13px;line-height:1.5;display:flex;flex-direction:column;gap:8px;'
const LABEL_CSS = 'display:block;font-weight:600;margin-top:4px;'
const HINT_CSS = 'display:block;color:var(--fg-muted,#57606a);font-size:12px;margin:2px 0;'
const INPUT_CSS =
  'width:100%;box-sizing:border-box;padding:6px 8px;font-size:13px;font-family:inherit;' +
  'border:1px solid rgba(127,127,127,.4);border-radius:6px;background:var(--bg,#fff);color:inherit;'
const BTN_CSS =
  'padding:6px 14px;font-size:13px;cursor:pointer;border:1px solid rgba(127,127,127,.4);' +
  'background:var(--bg,#f6f8fa);color:inherit;border-radius:6px;'
const BTN_PRIMARY_CSS =
  'padding:6px 14px;font-size:13px;cursor:pointer;border:none;background:#1f6feb;color:#fff;border-radius:6px;'
const TABLE_CSS = 'width:100%;border-collapse:collapse;font-size:12.5px;'
const CELL_CSS = 'padding:4px 8px;border-bottom:1px solid rgba(127,127,127,.2);text-align:left;white-space:nowrap;'
const PREVIEW_CSS =
  'max-height:320px;overflow:auto;padding:8px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;' +
  'font-size:12px;white-space:pre-wrap;word-break:break-all;border:1px solid rgba(127,127,127,.3);' +
  'border-radius:6px;background:var(--bg,#f6f8fa);margin:0;'
const STATUS_OK = 'color:#1a7f37;'
const STATUS_ERR = 'color:#cf222e;'

interface PanelHandle {
  root: HTMLElement
  refresh(): void
  setChrome(floating: boolean): void
}

function buildPanel(channel: SettingsChannel, rpcConn: any): PanelHandle {
  /* 状态区 */
  const status = el('div', { style: 'font-size:12.5px;', textContent: '' })
  const setStatus = (text: string, isError = false): void => {
    status.textContent = text
    status.setAttribute('style', `font-size:12.5px;${isError ? STATUS_ERR : STATUS_OK}`)
  }

  /* 集群配置区 */
  const clustersInput = el('textarea', { rows: '6', spellcheck: 'false', style: INPUT_CSS + 'font-family:ui-monospace,monospace;' }) as HTMLTextAreaElement
  const defaultClusterInput = el('input', { type: 'text', style: INPUT_CSS, placeholder: '留空取第一个' }) as HTMLInputElement
  const saveBtn = el('button', { type: 'button', textContent: '保存集群配置', style: BTN_PRIMARY_CSS }) as HTMLButtonElement
  const validateBtn = el('button', { type: 'button', textContent: '校验 JSON', style: BTN_CSS }) as HTMLButtonElement

  /* 浏览区 */
  const clusterSelect = el('select', { style: INPUT_CSS }) as HTMLSelectElement
  const pathInput = el('input', { type: 'text', style: INPUT_CSS, value: '/', spellcheck: 'false' }) as HTMLInputElement
  const goBtn = el('button', { type: 'button', textContent: '打开', style: BTN_PRIMARY_CSS }) as HTMLButtonElement
  const upBtn = el('button', { type: 'button', textContent: '⬆ 上级', style: BTN_CSS }) as HTMLButtonElement
  const refreshBtn = el('button', { type: 'button', textContent: '⟳ 刷新', style: BTN_CSS }) as HTMLButtonElement
  const summaryToggle = el('input', { type: 'checkbox' }) as HTMLInputElement
  const trashToggle = el('input', { type: 'checkbox' }) as HTMLInputElement
  trashToggle.checked = true

  const tableWrap = el('div', { style: 'overflow:auto;max-height:420px;' })
  const fileTable = el('table', { style: TABLE_CSS })
  tableWrap.append(fileTable)

  const previewBox = el('pre', { style: PREVIEW_CSS + 'display:none;' })
  const previewTitle = el('div', { style: 'font-weight:600;display:none;' })
  const previewClose = el('button', { type: 'button', textContent: '✕ 关闭预览', style: BTN_CSS + 'display:none;' }) as HTMLButtonElement

  const krbStatus = el('span', { style: 'font-size:12px;', textContent: '' })
  const krbLoginBtn = el('button', { type: 'button', textContent: '🔑 Kerberos 登录', style: BTN_CSS }) as HTMLButtonElement
  const pingBtn = el('button', { type: 'button', textContent: '测试连接', style: BTN_CSS }) as HTMLButtonElement

  let currentPath = '/'

  /** 当前集群 id（下拉选择优先，缺省用 defaultCluster）。 */
  function activeCluster(): string {
    return clusterSelect.value || ''
  }

  async function loadClusters(): Promise<void> {
    const value = (await callRpc(rpcConn, 'clusters', {})) as ClusterInfo
    clusterSelect.textContent = ''
    for (const c of value.clusters ?? []) {
      const opt = el('option', { value: c.id, textContent: `${c.id}（${c.url}）` })
      clusterSelect.append(opt)
    }
    if (!clusterSelect.value && (value.clusters?.length ?? 0) > 0) {
      clusterSelect.value = value.defaultCluster || value.clusters[0]!.id
    }
    if (value.clusterError) setStatus(`⚠ 配置解析失败：${value.clusterError}`, true)
  }

  async function ping(): Promise<void> {
    setStatus('连接测试中…')
    try {
      const id = activeCluster()
      await callRpc(rpcConn, 'ping', { cluster: id })
      setStatus(`✔ 集群 ${id} 连接正常`)
    } catch (err) {
      setStatus(`✘ ${err instanceof Error ? err.message : String(err)}`, true)
    }
  }

  async function krbStatusCheck(): Promise<void> {
    try {
      const value = (await callRpc(rpcConn, 'kerberosStatus', {})) as { ok: boolean; message: string }
      krbStatus.textContent = value.ok ? `✔ ${value.message}` : `○ ${value.message}`
      krbStatus.setAttribute('style', `font-size:12px;${value.ok ? STATUS_OK : 'color:var(--fg-muted,#57606a);'}`)
    } catch {
      krbStatus.textContent = '○ 状态未知'
    }
  }

  async function krbLogin(): Promise<void> {
    const id = activeCluster()
    setStatus(`Kerberos 登录中（${id}）…`)
    try {
      const value = (await callRpc(rpcConn, 'kerberosLogin', { cluster: id })) as { principal: string; ccachePath: string; message: string }
      setStatus(`✔ Kerberos 登录成功：${value.principal} → ${value.ccachePath}`)
      krbLoginBtn.textContent = '🔑 Kerberos 登录'
      krbLoginBtn.setAttribute('style', BTN_CSS)
      void krbStatusCheck()
    } catch (err) {
      setStatus(`✘ ${err instanceof Error ? err.message : String(err)}`, true)
    }
  }

  /** 列目录并渲染表格。 */
  async function browse(path?: string): Promise<void> {
    const target = (path ?? currentPath).trim() || '/'
    setStatus(`加载 ${target} …`)
    try {
      const value = (await callRpc(rpcConn, 'list', {
        cluster: activeCluster(),
        path: target,
        summary: summaryToggle.checked,
      })) as ListResult
      currentPath = value.path || target
      pathInput.value = currentPath
      renderRows(value)
      setStatus(`✔ ${currentPath}（${value.files.length} 项）`)
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      setStatus(`✘ ${message}`, true)
      // GSS/票据类失败：ccache 文件在磁盘上但 TGT 可能已过期（kerberosStatus 只看文件存在，
      // 会给虚假健康信号）。刷新状态并明确引导用户重新登录。
      if (/GSS|Kerberos|SASL|tgt|ccache|BAD_MECH/i.test(message)) {
        void krbStatusCheck()
        if (krbLoginBtn) {
          krbLoginBtn.textContent = '🔑 Kerberos 票据可能已过期，点此重新登录'
          krbLoginBtn.setAttribute('style', BTN_CSS + 'border-color:#cf222e;color:#cf222e;')
        }
      }
    }
  }

  function renderRows(result: ListResult): void {
    fileTable.textContent = ''
    const thead = el('thead', {}, [
      el('tr', {}, [
        el('th', { textContent: '名称' }),
        el('th', { textContent: '类型' }),
        el('th', { textContent: '权限' }),
        el('th', { textContent: '属主' }),
        el('th', { textContent: '大小' }),
        el('th', { textContent: '修改时间' }),
        el('th', { textContent: '操作' }),
      ]),
    ])
    for (const th of thead.querySelectorAll('th')) th.setAttribute('style', CELL_CSS)
    fileTable.append(thead)

    const tbody = el('tbody')
    for (const f of result.files) {
      const openBtn = el('button', {
        type: 'button',
        textContent: f.isdir ? '进入' : '预览',
        style: BTN_CSS + 'padding:2px 8px;font-size:12px;',
      }) as HTMLButtonElement
      openBtn.addEventListener('click', () => {
        if (f.isdir) void browse(f.path)
        else void preview(f.path)
      })
      const tr = el('tr', {}, [
        el('td', { textContent: f.name }),
        el('td', { textContent: f.isdir ? '📁' : '📄' }),
        el('td', { textContent: fmtPerm(f.permission) }),
        el('td', { textContent: f.owner ? `${f.owner}:${f.group}` : '-' }),
        el('td', { textContent: f.isdir ? (f.spaceConsumed != null ? fmtBytes(f.spaceConsumed) : '-') : fmtBytes(f.length) }),
        el('td', { textContent: fmtTime(f.modificationTime) }),
        el('td', {}, [openBtn]),
      ])
      for (const td of tr.querySelectorAll('td')) td.setAttribute('style', CELL_CSS)
      tbody.append(tr)
    }
    fileTable.append(tbody)
  }

  async function preview(path: string): Promise<void> {
    setStatus(`读取 ${path} …`)
    try {
      const value = (await callRpc(rpcConn, 'read', {
        cluster: activeCluster(),
        path,
        maxBytes: 256 * 1024,
      })) as { length: number; content: string; isorc: boolean; isparquet: boolean; isavro: boolean }
      const tags = [value.isorc && 'ORC', value.isparquet && 'Parquet', value.isavro && 'Avro'].filter(Boolean).join('/')
      previewTitle.textContent = `${path}（${fmtBytes(value.length)}${tags ? ` · ${tags}` : ''}）`
      previewBox.textContent = value.content || '(空文件或二进制内容)'
      for (const node of [previewTitle, previewBox, previewClose]) node.style.display = ''
      setStatus(`✔ 已读取 ${path}`)
    } catch (err) {
      setStatus(`✘ ${err instanceof Error ? err.message : String(err)}`, true)
    }
  }

  async function saveConfig(): Promise<void> {
    const raw = clustersInput.value.trim() || '[]'
    try {
      const parsed = JSON.parse(raw)
      if (!Array.isArray(parsed)) throw new Error('必须是 JSON 数组')
      for (const item of parsed) {
        if (typeof item?.url !== 'string' || !item.url.startsWith('hdfs://')) {
          throw new Error(`集群 url 必须以 hdfs:// 开头：${JSON.stringify(item?.url)}`)
        }
      }
    } catch (err) {
      setStatus(`✘ clusters JSON 无效：${err instanceof Error ? err.message : String(err)}`, true)
      return
    }
    try {
      await channel.write({ clusters: raw, defaultCluster: defaultClusterInput.value.trim() })
      setStatus('✔ 配置已保存（volatile 热更新，无需重启）')
      await loadClusters()
    } catch (err) {
      setStatus(`✘ 保存失败：${err instanceof Error ? err.message : String(err)}`, true)
    }
  }

  /**
   * 面板打开/刷新时的初始化。
   *
   * Kerberos 集群在此自动登录一次：ccache 文件在宿主重启后仍在磁盘上，
   * kerberosStatus 只看文件存在（会给虚假 ok），而 TGT 实际已随进程失效——
   * 不预登录的话面板一打开就是一片 GSS 报错。
   */
  async function refreshAll(): Promise<void> {
    try {
      await loadClusters()
      await krbStatusCheck()
      try {
        const value = await channel.read()
        if (typeof value.clusters === 'string') clustersInput.value = value.clusters
        defaultClusterInput.value = value.defaultCluster ?? ''
      } catch {
        // 读通道不可用不阻断浏览：集群下拉由 clusters 端点单独填充
      }
      await ensureKerberos()
      await browse(currentPath)
    } catch (err) {
      setStatus(`✘ ${err instanceof Error ? err.message : String(err)}`, true)
    }
  }

  /** 当前集群配了 principal+keytab 时，静默登录一次（失败不阻断，browse 会报真实错）。 */
  async function ensureKerberos(): Promise<void> {
    const raw = clustersInput.value.trim() || '[]'
    let list: Array<Record<string, unknown>> = []
    try {
      const parsed = JSON.parse(raw)
      if (Array.isArray(parsed)) list = parsed as Array<Record<string, unknown>>
    } catch {
      return
    }
    const id = activeCluster()
    const cluster = list.find((c) => c?.id === id)
    if (!cluster) return
    let extra: Record<string, unknown> = {}
    try {
      const parsedExtra = typeof cluster.extraConfig === 'string' ? JSON.parse(cluster.extraConfig) : cluster.extraConfig
      if (parsedExtra && typeof parsedExtra === 'object') extra = parsedExtra as Record<string, unknown>
    } catch {
      return
    }
    const hasKrb = Boolean(extra['dfs.namenode.kerberos.principal'] && extra['dfs.namenode.keytab.file'])
    if (!hasKrb) return
    try {
      await callRpc(rpcConn, 'kerberosLogin', { cluster: id })
      await krbStatusCheck()
    } catch {
      // 登录失败留给 browse 报错；这里不叠加噪音
    }
  }

  /* 事件绑定 */
  goBtn.addEventListener('click', () => void browse(pathInput.value))
  pathInput.addEventListener('keydown', (ev) => {
    if ((ev as KeyboardEvent).key === 'Enter') void browse(pathInput.value)
  })
  upBtn.addEventListener('click', () => {
    const p = currentPath.replace(/\/+$/, '')
    const parent = p === '' ? '/' : p.slice(0, p.lastIndexOf('/')) || '/'
    void browse(parent)
  })
  refreshBtn.addEventListener('click', () => void browse())
  summaryToggle.addEventListener('change', () => void browse())
  saveBtn.addEventListener('click', () => void saveConfig())
  validateBtn.addEventListener('click', () => {
    try {
      JSON.parse(clustersInput.value.trim() || '[]')
      setStatus('✔ JSON 格式正确')
    } catch (err) {
      setStatus(`✘ ${err instanceof Error ? err.message : String(err)}`, true)
    }
  })
  pingBtn.addEventListener('click', () => void ping())
  krbLoginBtn.addEventListener('click', () => void krbLogin())
  previewClose.addEventListener('click', () => {
    for (const node of [previewTitle, previewBox, previewClose]) node.style.display = 'none'
  })

  const root = el('div', { style: PANEL_CSS }, [
    el('div', { style: 'font-weight:700;font-size:14px;', textContent: '🐘 HDFS 浏览器' }),
    status,

    /* 浏览区 */
    el('div', { style: 'display:flex;gap:8px;align-items:center;flex-wrap:wrap;' }, [
      el('span', { style: 'font-weight:600;', textContent: '集群' }),
      clusterSelect,
      pingBtn,
      krbLoginBtn,
      krbStatus,
    ]),
    el('div', { style: 'display:flex;gap:8px;align-items:center;flex-wrap:wrap;' }, [
      pathInput,
      goBtn,
      upBtn,
      refreshBtn,
      el('label', { style: 'font-size:12px;cursor:pointer;display:flex;gap:4px;align-items:center;' }, [
        summaryToggle, el('span', { textContent: '目录汇总' }),
      ]),
      el('label', { style: 'font-size:12px;cursor:pointer;display:flex;gap:4px;align-items:center;' }, [
        trashToggle, el('span', { textContent: '删除进垃圾箱' }),
      ]),
    ]),
    tableWrap,
    previewTitle,
    previewBox,
    previewClose,

    /* 配置区 */
    el('div', { style: 'border-top:1px solid rgba(127,127,127,.25);margin-top:8px;padding-top:8px;' }, [
      el('span', { style: LABEL_CSS, textContent: 'HDFS 集群配置（JSON 数组）' }),
      el('span', {
        style: HINT_CSS,
        textContent:
          '[{"id":"prod","url":"hdfs://nameservice1","extraConfig":"{}"}]；' +
          'extraConfig 里可配 HA 参数与 Kerberos（dfs.namenode.kerberos.principal / dfs.namenode.keytab.file）',
      }),
      clustersInput,
      el('span', { style: LABEL_CSS, textContent: '默认集群 id' }),
      el('span', { style: HINT_CSS, textContent: '面板与 agent 工具省略 cluster 参数时使用；留空取第一个' }),
      defaultClusterInput,
      el('div', { style: 'display:flex;gap:8px;align-items:center;margin-top:6px;' }, [saveBtn, validateBtn]),
    ]),
  ])

  const handle: PanelHandle = {
    root,
    refresh: () => void refreshAll(),
    setChrome(floating: boolean) {
      root.style.cssText = floating
        ? PANEL_CSS +
          'position:fixed;right:16px;bottom:16px;z-index:2147483646;width:860px;max-height:80vh;' +
          'overflow:auto;padding:14px;background:var(--bg,#fff);border:1px solid rgba(127,127,127,.35);' +
          'border-radius:10px;box-shadow:0 8px 28px rgba(0,0,0,.25);'
        : PANEL_CSS
    },
  }
  return handle
}

/* ── 挂载进 dsh 设置页 ─────────────────────────────────────────────────── */

function settingsSlotOptions(React: any): Array<[string, Record<string, unknown>]> {
  const label = (): string => 'HDFS'
  return [
    ['settings.plugins.tab', { id: SETTINGS_NS, order: 20, label }],
    ['settings.plugin.item', { key: SETTINGS_NS, label }],
    ['settings.section', { id: PLUGIN_ID, label, title: 'HDFS 浏览器', icon: createNavIcon(React) }],
  ]
}

function createSettingsComponent(React: any, handle: PanelHandle): any {
  return function HdfsSettings() {
    const ref = React.useRef(null)
    React.useEffect(function () {
      const node = ref.current as HTMLElement | null
      if (!node) return
      handle.setChrome(false)
      node.appendChild(handle.root)
      handle.refresh()
    }, [])
    return React.createElement('div', { ref, 'data-dsh-hdfs-settings': 'true' })
  }
}

function createNavIcon(React: any): any {
  return function NavIcon(props: Record<string, unknown>) {
    return React.createElement(
      'svg',
      Object.assign({ width: 16, height: 16, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, 'aria-hidden': true }, props),
      React.createElement('path', { d: 'M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3z' }),
      React.createElement('path', { d: 'M4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6' }),
      React.createElement('path', { d: 'M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6' }),
    )
  }
}

function tryRegisterSettingsSlot(ctx: any, handle: PanelHandle, onSlotted: () => void): boolean {
  let wired = false
  let mounted = false

  const register = (slots: any): void => {
    if (wired || !slots || typeof slots.inject !== 'function') return
    let React: any
    try { React = require('react') } catch { return }
    if (!React || typeof React.createElement !== 'function') return
    wired = true

    const Comp = createSettingsComponent(React, handle)
    const options = settingsSlotOptions(React)
    const priority = new Map(options.map(([name], index) => [name, index]))
    const mountedAt = new Set<string>()
    const declared = (name: string): boolean => {
      if (typeof slots.specDynamic !== 'function') return false
      try { return Boolean(slots.specDynamic(name)) } catch { return false }
    }

    for (const [name, opts] of options) {
      try {
        slots.inject(name, () => {
          for (const [other] of options) {
            if (priority.get(other)! < priority.get(name)! && declared(other)) return
          }
          if (mountedAt.size > 0 && !mountedAt.has(name)) return
          const dispose = slots.register({ name, ...opts }, Comp)
          mountedAt.add(name)
          mounted = true
          onSlotted()
          return () => {
            mountedAt.delete(name)
            if (typeof dispose === 'function') dispose()
          }
        })
      } catch { /* 单个插槽注入失败不影响其余候选 */ }
    }
  }

  try {
    if (typeof ctx?.inject === 'function') ctx.inject(['slots'], (scope: any) => { register(scope?.slots) })
    register(service(ctx, 'slots'))
  } catch { /* 回退到浮动启动器 */ }
  return mounted
}

/* ── 回退挂载：浮动启动器 ──────────────────────────────────────────────── */

const SIDEBAR_SLOT = 'sidebar.footer.action'
const SIDEBAR_BTN_CSS =
  'display:flex;align-items:center;gap:6px;width:100%;box-sizing:border-box;' +
  'margin:4px 0;padding:8px 10px;font-size:13px;cursor:pointer;' +
  'border:1px solid rgba(127,127,127,.25);background:transparent;color:inherit;border-radius:8px;'
const FLOAT_BTN_CSS =
  'position:fixed;left:16px;bottom:16px;z-index:2147483647;padding:9px 14px;' +
  'font-size:13px;background:#1f6feb;color:#fff;border:none;border-radius:8px;' +
  'box-shadow:0 2px 8px rgba(0,0,0,.3);margin:0;'

function mountLauncher(launcher: HTMLButtonElement): () => void {
  let dead = false
  const styleSidebar = (): void => { launcher.style.cssText = SIDEBAR_BTN_CSS }
  const styleFloat = (): void => { launcher.style.cssText = FLOAT_BTN_CSS }

  function sidebarHost(): Element | null {
    return doc.querySelector(`[data-slot="${SIDEBAR_SLOT}"]`)
  }
  function ensureMounted(): void {
    if (dead) return
    const host = sidebarHost()
    if (host) {
      if (launcher.parentElement !== host) { host.append(launcher); styleSidebar() }
    } else if (launcher.parentElement !== doc.body) {
      doc.body.append(launcher); styleFloat()
    }
  }
  ensureMounted()
  const observer = new MutationObserver(() => ensureMounted())
  observer.observe(doc.documentElement, { childList: true, subtree: true })
  return () => { dead = true; observer.disconnect(); launcher.remove() }
}

/* ── 客户端插件契约 ────────────────────────────────────────────────────── */

function apply(ctx: any): void {
  if (win.__dshHdfsMounted === true) return
  const conn = service(ctx, 'connection')
  if (!conn || !conn.rpc || typeof conn.rpc.call !== 'function') return
  win.__dshHdfsMounted = true

  const channel = createChannel(ctx, conn)
  const handle = buildPanel(channel, conn)
  const panel = handle.root
  panel.id = 'dsh-hdfs-panel'

  let disposeLauncher: (() => void) | undefined
  if (tryRegisterSettingsSlot(ctx, handle, () => { disposeLauncher?.() })) return

  // 回退：设置页插槽不可用时保留浮动启动器
  handle.setChrome(true)
  panel.style.display = 'none'
  doc.body.append(panel)

  const launcher = el('button', {
    textContent: '🐘 HDFS',
    id: 'dsh-hdfs-launcher',
  }) as HTMLButtonElement

  launcher.addEventListener('click', () => {
    if (panel.style.display === 'none') {
      panel.style.display = 'flex'
      handle.refresh()
    } else {
      panel.style.display = 'none'
    }
  })

  disposeLauncher = mountLauncher(launcher)
}

module.exports = { name: PLUGIN_ID, inject: ['connection'], apply }
