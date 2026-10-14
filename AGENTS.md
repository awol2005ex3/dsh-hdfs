# AGENTS.md — dsh-hdfs 插件开发须知

本文件给后续在此仓库工作的 AI agent / 开发者。**动手前先读**，能避开本仓库踩过的全部坑。

## 1. 架构：三个半区

```
┌────────────────────┐   RPC(/api/hdfs/*)   ┌─────────────────┐   napi   ┌──────────────┐
│ 浏览器半 client.ts │ ◄──────────────────► │ 宿主半 index.ts │ ◄──────► │ Rust .node   │
│ (lib/client.js)    │   connection.rpc     │ (lib/index.js)  │ 直接调用 │ (lib/native) │
└───────────────────┘                      └────────────────┘          └──────────────┘
        │ 设置页插槽挂载                        │ connection.fetch.register 精确路由
        │ 配置走 configForms                    │ agent 工具 hdfs（复用同一分发器）
```

- **Rust 半**（`rust/`）：`hdfs-native` 0.11 的 napi-3 封装。每个调用新建 `Client`（无状态、无锁、无泄漏），代价是一次 TCP+握手。
- **宿主半**（`src/index.ts` + `src/rpc.ts` + `src/tool.ts` + `src/settings.ts`）：加载 `.node`，注册 20 条 `/api/hdfs/*` 精确路由，注册 `hdfs` agent 工具。
- **浏览器半**（`src/client.ts`）：编译成闭包工厂 bundle（`scripts/wrap-client.mjs`），挂设置页插槽，提供目录浏览/预览/集群配置/Kerberos 登录面板。

### 为什么 RPC 通道固定 `/api`

harness 0.1.5 起 `connection.rpc.handle` 对 out-of-tree 插件全部失效（内部 `owner.webServer` 访问抛错并被吞）。唯一正解是 `connection.fetch.register()` 挂 `/api/hdfs/<endpoint>` **精确路由**（exact 表优先于 `/api` prefix）。改通道 = 插件失联。

## 2. 用户名来源（用户明确要求，禁止改回配置字段）

| 场景 | 用户名来源 |
|---|---|
| Kerberos（配了 principal + keytab） | principal 的 `@` 前缀，如 `hdfs@XXX.COM` → `hdfs` |
| 非 Kerberos | 环境变量 `HADOOP_USER_NAME` → `HADOOP_USERNAME` → `HDFS_USERNAME` |

`build_client` 统一回写 `HADOOP_USER_NAME`（hdfs-native 只读这一个变量）。**不要把 user 加进 HdfsConfig**。

## 3. 已知坑（全部踩过，按出现频率排序）

### napi 边界用 camelCase

`#[napi(object)]` 结构体的字段在 JS 侧是 camelCase：`extra_config` → `extraConfig`、`timeout_ms` → `timeoutMs`。传 snake_case 会得到 `Error: Missing field extraConfig`。

### 所有 async napi 函数都返回 Promise

`kerberosLoginWithConfig` / `listStatus` / `readFile` 等都是 `async fn`，JS 侧**必须 await**。忘 await 时脚本会拿到 `Promise { <pending> }`，而 `JSON.stringify(promise)` 恰好输出 `{}`——一个极具迷惑性的假象。

### napi 构建的路径参数相对 `--cwd` 解析

`napi build --manifest-path rust/Cargo.toml` 实际找 `rust/rust/Cargo.toml`。全部路径用绝对值（见 package.json `build:native`）。

### `--js index.cjs` 必须显式指定

根 package.json 是 `"type": "module"`，napi 默认生成的 `.js` CJS 绑定在 ESM 上下文加载会炸（`ERR_INVALID_ARG_VALUE`）。

### cargo/napi/npm 在沙箱里跑不了

子进程 spawn 与 schannel 网络被沙箱阻断（`Access is denied`）。本机一律 `pwsh` + `sandbox_permissions: 'danger-full-access'`。

### `.node` 被宿主锁着时无法覆盖

Windows 上 dsh 宿主加载 `.node` 后文件锁死到进程退出。napi 的 `copyArtifact` 会报 `Internal Error: Failed to copy artifact`。**解法**：`cargo build --release` 单独编（产物在 `$CARGO_TARGET_DIR/release/dsh_hdfs_native.dll`），验证阶段可拷到临时目录加载测试；真正生效需重启宿主。

### `KRB5CCNAME=API:krb5cc` 陷阱（Windows 高发）

别的工具（JDK/MIT KFW）设的 `API:` 型 ccache 名 hdfs-native 解析不了，照抄还会把凭据写进 NTFS 备用数据流。`ccache_path()` 只接受 `FILE:` 前缀，否则用固定路径 `%USERPROFILE%\dsh_hdfs_ccache`。

### 宿主进程 ≠ 你的 shell 进程

Kerberos ccache 是进程级状态。在 pwsh 里测试通了，宿主里仍要重新 `kerberosLogin`（agent 工具里就是 `hdfs` 工具的 `kerberosLogin` 端点）。

### `ctx.logger` 是可调用访问器

`ctx.logger('hdfs').info(...)`。测试替身也要做成函数。

### cordis 4 严格服务访问

浏览器半只声明 `inject: ['connection']`。读 `configForms` / `slots` 必须 `ctx.get(name)`；属性访问会抛 `cannot get property ... without inject`，整个 fiber 进 failed。

### 配置通道不能缓存探测结果

`configForms` 的镜像异步就绪，插件启动时多半还不可用。**每次读写重新探测**（wecom 的教训：缓存一次失败 = 永久失去写能力）。用户报过「保存失败：当前 dsh 未提供 settings 表单服务」就是这个 bug。

### 浏览器 bundle 不能有 ESM 语法

tsc 产物带 `export {}` 会破坏 classic script 解析，必须过 `wrap-client.mjs` 包闭包工厂。

### PowerShell 改 UTF-8 文件会变乱码

`(Get-Content -Raw) -replace ... | Set-Content` 会把中文写成 mojibake。改文件内容一律用 edit/write 工具。

## 4. 验证矩阵

```bash
npm run build          # tsc + wrap-client
npm run check:native   # .node 可加载 + 19 个导出齐整
npm run check:plugin   # 宿主半：4 导出 + 20 路由 + 工具 + 信封 + disposer
npm run check:rpc      # 浏览器半：bundle 契约
npm run check          # 以上全部
```

真实集群端到端（不入库，配置放仓库外）：

```bash
# 集群配置 JSON（含 extraConfig）放任意路径，如仓库外目录
node scripts/e2e-native.mjs <cluster.json>
# 复现宿主环境（KRB5CCNAME=API:krb5cc）：
node scripts/e2e-isolated.mjs <cluster.json>
```

宿主内验证（agent 工具，配置已进 profile 后）：

```
hdfs version / clusters / kerberosLogin / ping / list / read / ...
```

## 5. 当前真实集群验证状态（2026-10-14）

- `hdfs_prod`（hdfs://nameservice1，HA 双 namenode，CDP Kerberos）全部通过：
  kerberosLogin ✔ / ping ✔ / list / ✔（18 项）/ list /tmp ✔（300+ 项）/ read ✔
- 测试集群配置写在 `C:\Users\Administrator\.dsh\profiles\web\cordis.patch.yml`
  （运行时配置，**不是代码**，可按需删）。仓库侧对应文件 `.e2e-cluster.local.json` 已 gitignore。
- Kerberos SASL/GSS 在本机可用：hdfs-native 经 libloading 找到了系统 gssapi 动态库。
