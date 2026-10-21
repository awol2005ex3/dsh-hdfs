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

/** 可排序列：与旧 hdfs-gui 的 el-table 列一一对应。 */
type SortKey = 'name' | 'type' | 'permission' | 'owner' | 'size' | 'mtime'

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
  /**
   * 布局裁剪：
   * - 'browse'：只留浏览区 + 关闭按钮（浮动浏览面板，独立页面）
   * - 'config'：只留配置区（设置页配置项，独立页面）
   * - true：浮动全量（兜底启动器）
   * - false：设置页内嵌全量
   * - 'tab'：紧凑排版（窄容器）
   */
  setChrome(mode: boolean | 'tab' | 'browse' | 'config'): void
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

  /* 文件操作工具栏：新建目录 / 新建文件 / 上传 / 删除所选 */
  const mkdirBtn = el('button', { type: 'button', textContent: '＋ 新建目录', title: '在当前目录新建文件夹', style: BTN_CSS }) as HTMLButtonElement
  const createFileBtn = el('button', { type: 'button', textContent: '＋ 新建文件', title: '在当前目录新建空白文件', style: BTN_CSS }) as HTMLButtonElement
  const uploadBtn = el('button', { type: 'button', textContent: '⬆ 上传文件', title: '从本机选择文件上传到当前目录', style: BTN_CSS }) as HTMLButtonElement
  const deleteBtn = el('button', { type: 'button', textContent: '🗑 删除所选', title: '删除表格中勾选的文件/目录', style: BTN_CSS + 'color:#cf222e;' }) as HTMLButtonElement
  /**
   * 浏览器文件选择器。选中的 File 没有宿主机路径，所以读取字节后以 base64
   * 走 `upload` RPC，由宿主落临时文件再交给原生模块分块上传。
   */
  const fileInput = el('input', { type: 'file', multiple: true, style: 'display:none;' }) as HTMLInputElement
  const MAX_UPLOAD_BYTES = 200 * 1024 * 1024

  let currentPath = '/'
  /** 勾选待删除的路径；换目录时清空，排序/重渲染时保留。 */
  const selectedPaths = new Set<string>()
  /** 当前表头的全选框（重渲染时重建）。 */
  let selectAllBox: HTMLInputElement | null = null

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

  /** 列目录并渲染表格；成功返回 true（操作类按钮据此决定是否覆盖状态提示）。 */
  async function browse(path?: string): Promise<boolean> {
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
      selectedPaths.clear()
      renderRows(value)
      setStatus(`✔ ${currentPath}（${value.files.length} 项）`)
      return true
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
      return false
    }
  }

  /** 排序状态：null = 原始（HDFS 返回）顺序。 */
  let sortKey: SortKey | null = null
  let sortDir: 'asc' | 'desc' = 'asc'
  let lastResult: ListResult | null = null

  /**
   * 排序取值。对齐旧 hdfs-gui：按数据字段排，不按格式化后的字符串
   * （大小按字节、时间按毫秒、权限按八进制位）。
   */
  function sortValue(f: HdfsFileRow, key: SortKey): string | number {
    switch (key) {
      case 'name': return f.name.toLowerCase()
      case 'type': return f.isdir ? 1 : 0
      case 'permission': return f.permission >>> 0
      case 'owner': return `${f.owner ?? ''}:${f.group ?? ''}`.toLowerCase()
      case 'size': return f.isdir ? (f.spaceConsumed ?? 0) : f.length
      case 'mtime': return f.modificationTime
    }
  }

  function sortedFiles(files: HdfsFileRow[]): HdfsFileRow[] {
    if (!sortKey) return files
    const key = sortKey
    const dir = sortDir === 'desc' ? -1 : 1
    return files.slice().sort((a, b) => {
      const av = sortValue(a, key)
      const bv = sortValue(b, key)
      if (av < bv) return -dir
      if (av > bv) return dir
      // 同值用名称兜底，避免每次点击后顺序漂移
      return a.name.localeCompare(b.name) || 0
    })
  }

  /** 点击表头：升序 → 降序 → 取消（恢复原始顺序），与 el-table 一致。 */
  function toggleSort(key: SortKey): void {
    if (sortKey !== key) {
      sortKey = key
      sortDir = 'asc'
    } else if (sortDir === 'asc') {
      sortDir = 'desc'
    } else {
      sortKey = null
      sortDir = 'asc'
    }
    if (lastResult) renderRows(lastResult)
  }

  function renderRows(result: ListResult): void {
    lastResult = result
    fileTable.textContent = ''
    const columns: Array<{ label: string; key?: SortKey }> = [
      { label: '' },
      { label: '名称', key: 'name' },
      { label: '类型', key: 'type' },
      { label: '权限', key: 'permission' },
      { label: '属主', key: 'owner' },
      { label: '大小', key: 'size' },
      { label: '修改时间', key: 'mtime' },
      { label: '操作' },
    ]
    const headRow = el('tr')
    for (const col of columns) {
      const active = Boolean(col.key) && sortKey === col.key
      const arrow = active ? (sortDir === 'asc' ? ' ▲' : ' ▼') : col.key ? ' ⇅' : ''
      const th = el('th', { textContent: col.label })
      if (!col.label) {
        // 勾选列：表头复选框全选/全不选当前目录
        const selectAll = el('input', { type: 'checkbox' }) as HTMLInputElement
        selectAll.title = '全选 / 全不选'
        selectAll.checked = result.files.length > 0 && result.files.every((f) => selectedPaths.has(f.path))
        selectAllBox = selectAll
        selectAll.addEventListener('change', () => {
          if (selectAll.checked) for (const f of result.files) selectedPaths.add(f.path)
          else selectedPaths.clear()
          renderRows(result)
        })
        th.append(selectAll)
      }
      if (col.key) {
        th.setAttribute(
          'style',
          CELL_CSS + 'cursor:pointer;user-select:none;' + (active ? 'color:#1f6feb;' : ''),
        )
        th.setAttribute('title', '点击排序：升序 → 降序 → 取消')
        th.setAttribute('aria-sort', active ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none')
        if (arrow) {
          const mark = el('span', { textContent: arrow, style: active ? '' : 'opacity:.45;' })
          th.append(mark)
        }
        th.addEventListener('click', () => toggleSort(col.key as SortKey))
      } else {
        th.setAttribute('style', CELL_CSS)
      }
      headRow.append(th)
    }
    fileTable.append(el('thead', {}, [headRow]))

    const tbody = el('tbody')
    for (const f of sortedFiles(result.files)) {
      const openBtn = el('button', {
        type: 'button',
        textContent: f.isdir ? '进入' : '预览',
        style: BTN_CSS + 'padding:2px 8px;font-size:12px;',
      }) as HTMLButtonElement
      openBtn.addEventListener('click', () => {
        if (f.isdir) void browse(f.path)
        else void preview(f.path)
      })
      const checkbox = el('input', { type: 'checkbox' }) as HTMLInputElement
      checkbox.checked = selectedPaths.has(f.path)
      checkbox.title = `选择 ${f.name}`
      checkbox.addEventListener('change', () => {
        if (checkbox.checked) selectedPaths.add(f.path)
        else selectedPaths.delete(f.path)
        updateSelectionUi()
      })
      const tr = el('tr', {}, [
        el('td', {}, [checkbox]),
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
    updateSelectionUi()
  }

  /** 工具栏删除按钮上的已选数量，并同步表头全选框。 */
  function updateSelectionUi(): void {
    const n = selectedPaths.size
    deleteBtn.textContent = n > 0 ? `🗑 删除所选（${n}）` : '🗑 删除所选'
    if (selectAllBox && lastResult) {
      selectAllBox.checked = lastResult.files.length > 0
        && lastResult.files.every((f) => selectedPaths.has(f.path))
    }
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

  /** 拼接当前目录与名称（根目录不产生双斜杠）。 */
  function joinPath(dir: string, name: string): string {
    return `${dir.replace(/\/+$/, '')}/${name}`
  }

  /** 弹窗输入名称；空名 / 含路径分隔符 / . 与 .. 一律拒绝。 */
  function promptName(title: string, placeholder: string): string | null {
    const raw = window.prompt(title, placeholder)
    if (raw === null) return null
    const name = raw.trim()
    if (!name) {
      setStatus('✘ 名称不能为空', true)
      return null
    }
    if (name === '.' || name === '..' || /[/\\]/.test(name)) {
      setStatus('✘ 名称不能包含路径分隔符，也不能是 . 或 ..', true)
      return null
    }
    return name
  }

  /** 新建目录（mkdrs -p 语义：父目录存在即可）。 */
  async function createFolder(): Promise<void> {
    const name = promptName('新建目录名称：', 'new_folder')
    if (!name) return
    const path = joinPath(currentPath, name)
    setStatus(`创建目录 ${path} …`)
    try {
      await callRpc(rpcConn, 'mkdirs', { cluster: activeCluster(), path })
      if (await browse()) setStatus(`✔ 已创建目录 ${path}`)
    } catch (err) {
      setStatus(`✘ ${err instanceof Error ? err.message : String(err)}`, true)
    }
  }

  /** 新建空白文件。 */
  async function createFile(): Promise<void> {
    const name = promptName('新建文件名称：', 'new_file.txt')
    if (!name) return
    const path = joinPath(currentPath, name)
    setStatus(`创建文件 ${path} …`)
    try {
      await callRpc(rpcConn, 'createEmpty', { cluster: activeCluster(), path })
      if (await browse()) setStatus(`✔ 已创建文件 ${path}`)
    } catch (err) {
      setStatus(`✘ ${err instanceof Error ? err.message : String(err)}`, true)
    }
  }

  /** 浏览器 File → base64（分块拼接，避免大文件撑爆函数参数栈）。 */
  function readFileBase64(file: File): Promise<string> {
    return new Promise((resolve, reject) => {
      const reader = new FileReader()
      reader.onerror = () => reject(reader.error ?? new Error('读取本地文件失败'))
      reader.onload = () => {
        const bytes = new Uint8Array(reader.result as ArrayBuffer)
        let binary = ''
        const step = 0x8000
        for (let i = 0; i < bytes.length; i += step) {
          binary += String.fromCharCode(...bytes.subarray(i, i + step))
        }
        resolve(btoa(binary))
      }
      reader.readAsArrayBuffer(file)
    })
  }

  /** 上传选中的本地文件到当前目录（逐个上传，失败即停并刷新已成功的部分）。 */
  async function uploadFiles(): Promise<void> {
    const files = Array.from(fileInput.files ?? [])
    fileInput.value = ''
    if (files.length === 0) return
    const dir = currentPath
    let done = 0
    for (const file of files) {
      if (file.size > MAX_UPLOAD_BYTES) {
        await browse(dir)
        setStatus(`✘ ${file.name} 过大（${fmtBytes(file.size)}，上限 ${fmtBytes(MAX_UPLOAD_BYTES)}）`, true)
        return
      }
      setStatus(`上传 ${file.name}（${done + 1}/${files.length}）…`)
      try {
        const base64 = await readFileBase64(file)
        await callRpc(rpcConn, 'upload', {
          cluster: activeCluster(),
          remoteDir: dir,
          name: file.name,
          base64,
          // 协议版本戳：宿主据此判断浏览器端是否还是旧代码（旧版只传空 localPath）。
          uploadProtocol: 2,
        })
        done += 1
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err)
        await browse(dir)
        setStatus(`✘ 上传 ${file.name} 失败（已成功 ${done} 个）：${message}`, true)
        return
      }
    }
    if (await browse(dir)) setStatus(`✔ 已上传 ${done} 个文件到 ${dir}`)
  }

  /** 删除勾选项：走 `delete`，是否进垃圾箱由工具栏开关决定。 */
  async function deleteSelected(): Promise<void> {
    const paths = Array.from(selectedPaths)
    if (paths.length === 0) {
      setStatus('✘ 请先在表格中勾选要删除的文件或目录', true)
      return
    }
    const trash = trashToggle.checked
    const tip = trash ? '将移入 HDFS 垃圾箱（可恢复）' : '⚠ 永久删除，跳过垃圾箱'
    const confirmed = window.confirm(
      `确定删除以下 ${paths.length} 项？\n\n${paths.join('\n')}\n\n${tip}`,
    )
    if (!confirmed) return
    setStatus(`删除 ${paths.length} 项 …`)
    try {
      const value = (await callRpc(rpcConn, 'delete', {
        cluster: activeCluster(),
        paths,
        trash,
      })) as { removed: number }
      selectedPaths.clear()
      if (await browse()) setStatus(`✔ 已删除 ${value.removed} 项`)
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
  mkdirBtn.addEventListener('click', () => void createFolder())
  createFileBtn.addEventListener('click', () => void createFile())
  uploadBtn.addEventListener('click', () => fileInput.click())
  fileInput.addEventListener('change', () => void uploadFiles())
  deleteBtn.addEventListener('click', () => void deleteSelected())
  previewClose.addEventListener('click', () => {
    for (const node of [previewTitle, previewBox, previewClose]) node.style.display = 'none'
  })

  const browseZone = el('div', { id: 'dsh-hdfs-browse-zone' }, [
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
    ]),
    /* 文件操作工具栏（对应旧版 hdfs-gui 的新建目录/新建文件/上传/删除） */
    el('div', { style: 'display:flex;gap:8px;align-items:center;flex-wrap:wrap;' }, [
      mkdirBtn,
      createFileBtn,
      uploadBtn,
      deleteBtn,
      el('label', { style: 'font-size:12px;cursor:pointer;display:flex;gap:4px;align-items:center;' }, [
        trashToggle, el('span', { textContent: '删除进垃圾箱' }),
      ]),
      fileInput,
    ]),
    tableWrap,
    previewTitle,
    previewBox,
    previewClose,
  ])

  const configZone = el('div', {
    id: 'dsh-hdfs-config-zone',
    style: 'border-top:1px solid rgba(127,127,127,.25);margin-top:8px;padding-top:8px;',
  }, [
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
  ])

  /* 标题行：标题文字 + 最大化/关闭按钮（仅浮动浏览面板显示） */
  let maximized = false
  /** 未最大化时记住的浮动几何；首次显示时用默认值填充，之后拖动/resize 实时更新。 */
  let savedRect = { left: -1, top: -1, width: 860, height: 560 }
  let rectSeeded = false

  const closeBtn = el('button', {
    type: 'button',
    title: '关闭',
    id: 'dsh-hdfs-close',
    textContent: '✕',
    style: 'margin-left:auto;padding:2px 8px;font-size:13px;cursor:pointer;' +
      'border:1px solid rgba(127,127,127,.3);background:transparent;color:inherit;border-radius:6px;display:none;',
  }) as HTMLButtonElement
  closeBtn.addEventListener('click', () => {
    root.style.display = 'none'
  })

  const maximizeBtn = el('button', {
    type: 'button',
    title: '最大化 / 还原',
    id: 'dsh-hdfs-maximize',
    textContent: '⤢',
    style: 'margin-left:8px;padding:2px 8px;font-size:13px;cursor:pointer;' +
      'border:1px solid rgba(127,127,127,.3);background:transparent;color:inherit;border-radius:6px;display:none;',
  }) as HTMLButtonElement

  /** 把 root 的当前布局固化成 left/top/width/height（display:none 时拿不到矩形，跳过）。 */
  function seedRect(): void {
    if (root.style.display === 'none') return
    const rect = root.getBoundingClientRect()
    if (rect.width < 40 || rect.height < 40) return
    savedRect = { left: rect.left, top: rect.top, width: rect.width, height: rect.height }
    rectSeeded = true
  }

  /** 把 left/top 锚定（right/bottom → auto），为拖动/resize 做准备。 */
  function anchorLeftTop(): void {
    if (root.style.right !== 'auto') {
      seedRect()
      root.style.right = 'auto'
      root.style.bottom = 'auto'
      root.style.left = `${savedRect.left}px`
      root.style.top = `${savedRect.top}px`
      root.style.width = `${savedRect.width}px`
      root.style.height = `${savedRect.height}px`
    }
  }

  function applyMaximize(): void {
    if (maximized) {
      seedRect() // 记住最大化前的几何（仅首次有效，拖动/resize 已实时更新 savedRect）
      root.style.left = '24px'
      root.style.top = '24px'
      root.style.right = '24px'
      root.style.bottom = '24px'
      root.style.width = 'auto'
      root.style.height = 'auto'
      root.style.maxHeight = 'none'
      maximizeBtn.textContent = '⤣'
      maximizeBtn.title = '还原'
      tableWrap.style.maxHeight = 'calc(100vh - 230px)'
    } else {
      // 还原：回到 savedRect（拖动/resize 后它始终是最新的浮动几何）
      root.style.left = `${savedRect.left}px`
      root.style.top = `${savedRect.top}px`
      root.style.width = `${savedRect.width}px`
      root.style.height = `${savedRect.height}px`
      root.style.right = 'auto'
      root.style.bottom = 'auto'
      root.style.maxHeight = '80vh'
      maximizeBtn.textContent = '⤢'
      maximizeBtn.title = '最大化'
      tableWrap.style.maxHeight = '55vh'
    }
  }
  maximizeBtn.addEventListener('click', () => {
    maximized = !maximized
    applyMaximize()
  })

  /* ── 标题栏拖动（最大化时禁用） ───────────────────────────────────────── */
  const headingLabel = el('span', { textContent: '🐘 HDFS 浏览器', style: 'user-select:none;' })
  const headingRow = el('div', {
    id: 'dsh-hdfs-heading',
    style: 'display:flex;align-items:center;font-weight:700;font-size:14px;' +
      'cursor:move;flex-shrink:0;',
  }, [headingLabel, maximizeBtn, closeBtn])

  headingRow.addEventListener('mousedown', (ev) => {
    if (maximized) return
    const mouse = ev as MouseEvent
    if (mouse.button !== 0) return
    ev.preventDefault()
    anchorLeftTop()
    const startLeft = savedRect.left
    const startTop = savedRect.top
    const offsetX = mouse.clientX - startLeft
    const offsetY = mouse.clientY - startTop

    const onMove = (e: MouseEvent): void => {
      // 至少保留 80px 标题在视口内，防止拖出屏幕后无法找回
      const x = Math.min(Math.max(e.clientX - offsetX, 80 - savedRect.width), window.innerWidth - 80)
      const y = Math.min(Math.max(e.clientY - offsetY, 0), window.innerHeight - 40)
      savedRect.left = x
      savedRect.top = y
      root.style.left = `${x}px`
      root.style.top = `${y}px`
    }
    const onUp = (): void => {
      doc.removeEventListener('mousemove', onMove)
      doc.removeEventListener('mouseup', onUp)
    }
    doc.addEventListener('mousemove', onMove)
    doc.addEventListener('mouseup', onUp)
  })

  /* ── 右下角拖拽改变大小（最大化时禁用） ───────────────────────────────── */
  const resizeGrip = el('div', {
    title: '拖拽改变大小',
    id: 'dsh-hdfs-resize-grip',
    style: 'position:absolute;right:0;bottom:0;width:16px;height:16px;cursor:nwse-resize;' +
      'background:linear-gradient(135deg,transparent 50%,rgba(127,127,127,.45) 50%);' +
      'border-bottom-right-radius:10px;display:none;',
  })
  resizeGrip.addEventListener('mousedown', (ev) => {
    if (maximized) return
    const mouse = ev as MouseEvent
    if (mouse.button !== 0) return
    ev.preventDefault()
    ev.stopPropagation()
    anchorLeftTop()
    const startW = savedRect.width
    const startH = savedRect.height
    const startX = mouse.clientX
    const startY = mouse.clientY

    const onMove = (e: MouseEvent): void => {
      // 最小 480x360，最大不超出视口
      const w = Math.min(Math.max(e.clientX - startX + startW, 480), window.innerWidth - savedRect.left - 8)
      const h = Math.min(Math.max(e.clientY - startY + startH, 360), window.innerHeight - savedRect.top - 8)
      savedRect.width = w
      savedRect.height = h
      root.style.width = `${w}px`
      root.style.height = `${h}px`
    }
    const onUp = (): void => {
      doc.removeEventListener('mousemove', onMove)
      doc.removeEventListener('mouseup', onUp)
    }
    doc.addEventListener('mousemove', onMove)
    doc.addEventListener('mouseup', onUp)
  })

  const root = el('div', { style: PANEL_CSS }, [headingRow, status, browseZone, configZone, resizeGrip])

  const handle: PanelHandle = {
    root,
    refresh: () => void refreshAll(),
    /**
     * 布局裁剪：
     * - 'browse'：只留浏览区 + 关闭按钮（浮动浏览面板）
     * - 'config'：只留配置区（设置页配置项）
     * - true：浮动全量（兜底启动器用）
     * - false：设置页内嵌全量
     * - 'tab'：紧凑排版（窄容器）
     */
    setChrome(mode: boolean | 'tab' | 'browse' | 'config') {
      const browseOnly = mode === 'browse'
      const configOnly = mode === 'config'
      if (browseOnly || configOnly) {
        // 拆分区：浏览/配置各自独立页面，互不干扰
        browseZone.style.display = browseOnly ? '' : 'none'
        configZone.style.display = configOnly ? '' : 'none'
        status.style.display = ''
        closeBtn.style.display = browseOnly ? '' : 'none'
        maximizeBtn.style.display = browseOnly ? '' : 'none'
        const label = headingRow.firstElementChild as HTMLElement | null
        if (label) label.style.display = ''
      }
      if (mode === 'browse') {
        root.style.cssText =
          'color:var(--fg,#1f2328);font-size:13px;line-height:1.5;display:flex;flex-direction:column;gap:8px;' +
          'position:fixed;right:16px;bottom:16px;z-index:2147483646;width:860px;max-height:80vh;' +
          'overflow:auto;padding:14px;background:var(--bg,#fff);border:1px solid rgba(127,127,127,.35);' +
          'border-radius:10px;box-shadow:0 8px 28px rgba(0,0,0,.25);'
        resizeGrip.style.display = ''
        tableWrap.style.maxHeight = '55vh'
      } else if (mode === 'config') {
        root.style.cssText = PANEL_CSS
        closeBtn.style.display = 'none'
        maximizeBtn.style.display = 'none'
        resizeGrip.style.display = 'none'
        tableWrap.style.maxHeight = '420px'
      } else if (mode === 'tab') {
        // 紧凑排版（窄容器）
        root.style.cssText =
          'color:var(--fg,#1f2328);font-size:12.5px;line-height:1.45;display:flex;' +
          'flex-direction:column;gap:6px;padding:2px;'
        browseZone.style.display = ''
        configZone.style.display = ''
        closeBtn.style.display = 'none'
        maximizeBtn.style.display = 'none'
        resizeGrip.style.display = 'none'
        const label = headingRow.firstElementChild as HTMLElement | null
        if (label) label.style.display = 'none'
        tableWrap.style.maxHeight = '50vh'
      } else if (mode) {
        root.style.cssText = PANEL_CSS +
          'position:fixed;right:16px;bottom:16px;z-index:2147483646;width:860px;max-height:80vh;' +
          'overflow:auto;padding:14px;background:var(--bg,#fff);border:1px solid rgba(127,127,127,.35);' +
          'border-radius:10px;box-shadow:0 8px 28px rgba(0,0,0,.25);'
        browseZone.style.display = ''
        configZone.style.display = ''
        closeBtn.style.display = ''
        maximizeBtn.style.display = ''
        resizeGrip.style.display = ''
        const label = headingRow.firstElementChild as HTMLElement | null
        if (label) label.style.display = ''
        tableWrap.style.maxHeight = '420px'
      } else {
        root.style.cssText = PANEL_CSS
        browseZone.style.display = ''
        configZone.style.display = ''
        closeBtn.style.display = 'none'
        maximizeBtn.style.display = 'none'
        resizeGrip.style.display = 'none'
        const label = headingRow.firstElementChild as HTMLElement | null
        if (label) label.style.display = ''
        tableWrap.style.maxHeight = '420px'
      }
    },
  }
  return handle
}

/* ── 挂载策略（按优先级逐级降级） ──────────────────────────────────────────
   1. 侧边栏 footer 按钮（`sidebar.footer.action`）+ 栏内展开面板——根作用域
      插槽，体验最接近「点开就有目录」。
   2. 设置页插槽（settings.plugins.tab → plugin.item → section）。
   3. 浮动启动器 + 浮动面板（DOM 兜底）。

   ⚠ 已废弃：`sidebar.right.pane.tab` 侧边栏 tab（2026-10-14 实测不可行）。
   该插槽由 `rightbar.session` 条目（session 作用域）声明，根 fiber 的
   slots.inject 回调永不触发；附带服务 `sidebarRightTabs` / `sidebarRight`
   在兄弟 fiber 同样不可见（probe 实测全 false）。插槽代码已删，将来 dsh
   若把该插槽提升到根作用域可参考 git 历史恢复。
   ────────────────────────────────────────────────────────────────────── */

/* ── 回退 A：设置页插槽 ────────────────────────────────────────────────── */

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
      // 设置页只放配置区（浏览是另一个页面——侧边栏按钮 + 浮动面板）
      handle.setChrome('config')
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

/* ── 挂载策略（按优先级逐级降级） ──────────────────────────────────────────
   1. 侧边栏 footer 按钮（`sidebar.footer.action` 插槽）+ 侧边栏内展开面板——
      根作用域插槽，探测可用；体验最接近「像工作区文件那样点开就有目录」。
   2. 设置页插槽（settings.plugins.tab → plugin.item → section）。
   3. 浮动启动器 + 浮动面板（DOM 兜底）。

   ⚠ 已废弃：`sidebar.right.pane.tab` 侧边栏 tab。该插槽由 `rightbar.session`
   条目（session 作用域）声明，根 fiber 的 slots.inject 回调永不触发
   （2026-10-14 实测：只有 probe 日志、没有 declared 日志）。附带服务
   `sidebarRightTabs` / `sidebarRight` 在兄弟 fiber 同样不可见。若将来 dsh
   把该插槽提升到根作用域，或本插件的 fiber 能注入这些服务，可恢复。
   ────────────────────────────────────────────────────────────────────── */

const SIDEBAR_FOOTER_SLOT = 'sidebar.footer.action'

/**
 * 侧边栏 footer 按钮（`sidebar.footer.action` 插槽）。
 *
 * 契约事实（cordis-client-runner SlotMap）：
 * - `kind: "list"`：**列表插槽**，每个条目是「Settings 旁的一个小按钮」，
 *   不是容器——把面板塞进来会撞 React error #62（对象被当 child）。
 * - 组件签名只收 owner 的 `{ wide }`（侧边栏宽栏/56px rail）。
 * - `scope: "root"`：根作用域，slots.inject 回调立即触发。
 *
 * 因此按钮只做「开关」，点击切**浮动面板**的显示（面板是纯 DOM，
 * 不经过 React，无任何渲染契约风险）。
 */
function createSidebarButton(React: any, handle: PanelHandle): any {
  const Icon = createNavIcon(React)
  return function HdfsSidebarButton({ wide }: { wide?: boolean }): any {
    const [open, setOpen] = React.useState(false)
    React.useEffect(function () {
      // 打开时把浮动面板显示出来；关闭时隐藏（面板 DOM 由 apply 预挂到 body）
      handle.root.style.display = open ? 'flex' : 'none'
      if (open) handle.refresh()
    }, [open])
    return React.createElement(
      'button',
      {
        type: 'button',
        title: 'HDFS 浏览器',
        'data-dsh-hdfs-sidebar-toggle': 'true',
        'aria-expanded': open ? 'true' : 'false',
        onClick: () => setOpen((v: boolean) => !v),
        style: {
          display: 'flex', alignItems: 'center', gap: wide === false ? '0' : '8px',
          justifyContent: wide === false ? 'center' : 'flex-start',
          width: '100%', boxSizing: 'border-box', margin: '2px 0', padding: '8px 10px',
          fontSize: '13px', cursor: 'pointer',
          border: '1px solid rgba(127,127,127,.25)',
          background: open ? 'rgba(127,127,127,.15)' : 'transparent',
          color: 'inherit', borderRadius: '8px',
        },
      },
      React.createElement(Icon, null),
      wide === false ? null : 'HDFS',
    )
  }
}

/**
 * 侧边栏 footer 按钮路线：注册 `sidebar.footer.action` 插槽。
 *
 * 面板本体由调用方预挂为浮动面板（apply 里 buildPanel + body.append），
 * 这里只注册按钮；注册成功返回 true。
 */
function tryRegisterSidebarEntry(ctx: any, handle: PanelHandle): boolean {
  const slots = service(ctx, 'slots')
  if (!slots || typeof slots.inject !== 'function') return false
  let React: any
  try { React = require('react') } catch { return false }
  if (!React || typeof React.createElement !== 'function') return false
  console.info('[dsh-hdfs] registering sidebar.footer.action button (root-scope list slot)')
  try {
    ctx.effect(() => slots.inject(SIDEBAR_FOOTER_SLOT, () => {
      const Button = createSidebarButton(React, handle)
      return slots.register({
        name: SIDEBAR_FOOTER_SLOT,
        id: PLUGIN_ID,
        order: 50,
        label: 'HDFS',
      }, Button)
    }), 'hdfs: sidebar footer button')
    return true
  } catch (err) {
    console.warn('[dsh-hdfs] sidebar footer registration failed:', err)
    return false
  }
}

/* ── 客户端插件契约 ────────────────────────────────────────────────────── */

function apply(ctx: any): void {
  if (win.__dshHdfsMounted === true) return
  const conn = service(ctx, 'connection')
  if (!conn || !conn.rpc || typeof conn.rpc.call !== 'function') return
  win.__dshHdfsMounted = true

  const channel = createChannel(ctx, conn)

  // 布局策略（2026-10-14 定稿）：
  // - **浏览面板**：浮动、只含浏览区（集群下拉/路径/表格/预览/Kerberos），
  //   右上角带 ✕ 关闭按钮；由侧边栏 footer 按钮（`sidebar.footer.action`，
  //   根作用域 list 插槽，注册为纯按钮组件）切换显示；
  // - **配置面板**：设置页插槽挂载，只含配置区（clusters JSON + defaultCluster
  //   + 保存/校验）——与浏览页彻底分离，两个独立页面；
  // - 两者持**独立**面板实例（buildPanel 是纯 DOM 工厂，共用同一份
  //   handle.root 会互相搬走 DOM——曾导致「配置页面不见了」）；
  // - 两个插槽都注册失败时，左下浮动按钮兜底控制浏览面板。
  let entryCount = 0

  const browseHandle = buildPanel(channel, conn)
  browseHandle.root.id = 'dsh-hdfs-browse-panel'
  browseHandle.setChrome('browse')
  browseHandle.root.style.display = 'none'
  doc.body.append(browseHandle.root)
  if (tryRegisterSidebarEntry(ctx, browseHandle)) entryCount += 1

  const configHandle = buildPanel(channel, conn)
  if (tryRegisterSettingsSlot(ctx, configHandle, () => {})) entryCount += 1

  // 兜底：两个插槽都没挂上时，浮动按钮直接控制那份已挂好的浏览面板
  if (entryCount === 0) {
    const panel = browseHandle.root
    const launcher = el('button', {
      textContent: '🐘 HDFS',
      id: 'dsh-hdfs-launcher',
      style: 'position:fixed;left:16px;bottom:16px;z-index:2147483647;padding:9px 14px;' +
        'font-size:13px;background:#1f6feb;color:#fff;border:none;border-radius:8px;' +
        'box-shadow:0 2px 8px rgba(0,0,0,.3);margin:0;cursor:pointer;',
    }) as HTMLButtonElement

    launcher.addEventListener('click', () => {
      if (panel.style.display === 'none') {
        panel.style.display = 'flex'
        browseHandle.refresh()
      } else {
        panel.style.display = 'none'
      }
    })

    doc.body.append(launcher)
  }
}

module.exports = { name: PLUGIN_ID, inject: ['connection', 'slots'], apply }
