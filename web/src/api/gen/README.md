# src/api/gen —— 从 api/proto/*.proto 生成，不要手改

重新生成：

```bash
npm run gen:api        # 在 web/ 下
```

## 四个选项都是必需的，缺一个就会静默错位

生成的类型必须**逐字匹配网关实际发出的 JSON**。网关用的是：

```go
// cmd/stratum-gateway/main.go
marshalOpts = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}
```

对应到 ts-proto 上就是这四个 flag：

| flag | 为什么不能省 |
|---|---|
| `useJsonWireFormat=true` | 让枚举是**字符串**（`"INDEX_STATUS_READY"`）。默认生成数字枚举（`INDEX_STATUS_READY = 1`），而 protojson 发的是字符串——类型不会报错，运行时全是错的值。 |
| `snakeToCamel=false` | 字段名保持 `version_id`。默认会变成 `versionId`，而线上就是 `version_id`——读到的全是 `undefined`。 |
| `forceLong=string` | int64 用 `string`。protojson 把 int64 编成 JSON **字符串**（实测 `{"version_id":"12"}`），ts-proto 默认给 `number`——于是 `version_id === 12` 恒为 false，`version_id + 1` 变成字符串拼接。 |
| `onlyTypes=true` | 只生成类型，不带 encode/decode 运行时。前端消费的是 JSON，不需要 protobuf 运行时。 |

一条命令自检生成结果对不对：

```bash
grep -A3 'export interface QueryResponse' src/api/gen/query.ts
# 期望：version_id: string;  storage_degraded: boolean;
```

## EmitUnpopulated 的一个后果

网关设了 `EmitUnpopulated: true`，所以**零值字段也会出现**——枚举总是显式的字符串，不用拿"字段缺失"反推零值。这对前端是好事：不需要为"字段可能不存在"写分支。

## 为什么只生成这三个文件

`query.proto` / `knowledgebase.proto` / `admin.proto` 是网关对外暴露的三个服务。`internal.proto`、`sync.proto`、`kvraft/`、`vecstore/` 是集群内部协议，网关有意不转发（见 README 的服务边界一节），前端不生成它们。
