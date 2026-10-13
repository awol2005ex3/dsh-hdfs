/**
 * dsh-hdfs 的 agent 工具。
 *
 * 设计：不直接暴露十几个细粒度工具（工具面越大模型选择越难），而是
 * 「一个万能 hdfs 代理工具 + 结构化 endpoint 参数」。执行体直接复用 RPC 分发器，
 * 与浏览器半走完全相同的校验与错误路径。
 */
import { defineTool } from '@deepseek-ai/dsh-tools'
import { RPC_ENDPOINTS } from './rpc.js'

export type RpcDispatch = (endpoint: string, args: Record<string, unknown>) => Promise<unknown>

/** 输出 JSON 截断上限（工具结果过大会挤爆上下文）。 */
const OUTPUT_LIMIT = 256 * 1024

export function createHdfsTool(dispatch: RpcDispatch) {
  return defineTool({
    name: 'hdfs',
    description: [
      'Operate HDFS clusters configured in the dsh-hdfs plugin (list/read/write/mkdir/rename/delete/chmod/acl/kerberos/search).',
      'Pass `endpoint` (one of: ' + RPC_ENDPOINTS.join(', ') + ') and `args` matching it.',
      'Examples: {endpoint:"list",args:{path:"/data"}} · {endpoint:"read",args:{path:"/data/a.csv",maxBytes:65536}} ·',
      '{endpoint:"findSpecialDirs",args:{patterns:[".Trash","staging"]}} · {endpoint:"clusters",args:{}} to list configured clusters.',
      'Omitting `cluster` uses the default cluster; Kerberos clusters login automatically via keytab from cluster config.',
    ].join(' '),
    parameters: {
      endpoint: {
        type: 'string',
        required: true,
        description: `RPC endpoint, one of: ${RPC_ENDPOINTS.join(', ')}`,
      },
      args: {
        type: 'object',
        additionalProperties: true,
        description: 'Endpoint arguments (path, cluster, content, paths, permission, patterns, ...).',
      },
      cluster: {
        type: 'string',
        description: 'Cluster id. Defaults to the plugin default cluster.',
      },
    },
    output: {
      schema: {
        type: 'object',
        additionalProperties: false,
        properties: {
          endpoint: { type: 'string' },
          ok: { type: 'boolean' },
          result: { type: 'string', description: 'JSON result (truncated to 256KB)' },
        },
      },
      render: (_args, value) => [
        {
          type: 'text',
          text: value.ok ? `hdfs ${value.endpoint} → ${value.result}` : `hdfs ${value.endpoint} failed → ${value.result}`,
        },
      ],
    },
    async execute(args, exec) {
      if (exec.signal?.aborted) throw new Error('Cancelled')
      const endpoint = String(args.endpoint ?? '')
      if (!RPC_ENDPOINTS.includes(endpoint as (typeof RPC_ENDPOINTS)[number])) {
        throw new Error(`unknown endpoint "${endpoint}". Valid: ${RPC_ENDPOINTS.join(', ')}`)
      }
      const rpcArgs: Record<string, unknown> =
        typeof args.args === 'object' && args.args !== null ? { ...(args.args as Record<string, unknown>) } : {}
      if (typeof args.cluster === 'string' && args.cluster) rpcArgs.cluster = args.cluster

      try {
        const value = await dispatch(endpoint, rpcArgs)
        let json = JSON.stringify(value, null, 2)
        if (json.length > OUTPUT_LIMIT) {
          json = `${json.slice(0, OUTPUT_LIMIT)}\n…[truncated ${json.length - OUTPUT_LIMIT} chars]`
        }
        return { endpoint, ok: true, result: json }
      } catch (err) {
        // 错误也走结构化返回（ok:false），模型能读到完整错误消息并自行纠正参数。
        return {
          endpoint,
          ok: false,
          result: err instanceof Error ? err.message : String(err),
        }
      }
    },
  })
}
