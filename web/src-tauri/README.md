# Stratum 桌面控制台（Tauri）

这是同一套 React 界面的**桌面壳**。它存在的理由只有一个：

> 让关键数据落在一个不会随浏览器消失的地方。

## 为什么需要它

`client/pending.go` 的注释写明了那件事：**集群不替任何人保存 changes**。本进程
之外的唯一副本，要等协调者写下 WAL BEGIN 才出现——在那之前，每一次提交的数据
在世界上只有调用方手里有。所以调用方必须自己留一份记录，它是"重发还是放弃"的
唯一依据（`docs/client-integration-guide.md` §7、§8）。

浏览器版本把这份记录放在 **IndexedDB** 里，而它会被这些东西静默抹掉：

- 清浏览器数据 / 清缓存
- 隐私模式（直接不可用）
- 换浏览器、换设备
- 浏览器自己的存储回收策略

用户既不知道它存在，也不会去备份它。**这份桌面壳把它换成 `app_data_dir` 下的
JSON 文件**——用户能看见、能备份、也不随浏览器策略消失。

## 数据存在哪

启动后，「在途批次」面板底部会显示实际路径（`store_dir` 命令）。各平台的默认位置：

| 平台 | 路径 |
|---|---|
| Linux | `~/.local/share/com.stratum.console/` |
| macOS | `~/Library/Application Support/com.stratum.console/` |
| Windows | `%APPDATA%\com.stratum.console\` |

里面有：

| 文件 | 内容 | 损坏时的策略 |
|---|---|---|
| `pending.json` | 在途批次（changes + 幂等键） | **报错、且拒绝写入**——覆盖一个损坏的文件等于把用户仅剩的记录也抹掉 |
| `settings.json` | 本地偏好（gateway 地址、选中的库、top-k…） | 退回默认值（偏好是可重建的） |
| `history.jsonl` | 操作历史（每行一条，追加式） | 跳过坏行（一行损坏不该让整段历史读不出来） |

三个文件都用**原子写**（同目录 sibling temp + `fsync` + `rename`），理由与
`client/pending.go` 相同：这个机制存在的理由就是崩溃，半写的状态文件会恰好丢掉
那些要紧的记录。

## 前置依赖

**Linux**（WebKit 是 Tauri 在 Linux 上的 WebView）：

```bash
sudo apt-get update && sudo apt-get install -y \
  libwebkit2gtk-4.1-dev build-essential curl wget file \
  libxdo-dev libssl-dev libayatana-appindicator3-dev librsvg2-dev
```

**Rust**：`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

## 跑起来

```bash
cd web
npm run desktop:dev      # 开发（Vite dev server + 桌面窗口）
npm run desktop:build    # 打包（产物在 src-tauri/target/release/bundle/）
```

打包**不会**自带后端。桌面窗口要连一个跑着的 gateway——地址在 `settings.json` 的
`gateway_url`（默认 `http://127.0.0.1:8081`）。仓库根目录的 `./start.sh` 会拉起
完整链路（vecstore → stratum → 服务站 → gateway）。

## 两个环境下的差异

同一份前端代码跑在两种环境里，差异只有两处，都收在 `src/desktop/tauri.ts` 与
`src/api/client.ts` 里：

|  | 浏览器（gateway 的 `-static`） | 桌面（Tauri） |
|---|---|---|
| 本机持久化 | IndexedDB（易失） | `app_data_dir` 的 JSON 文件 |
| API 地址 | 同源相对路径 `/api/…` | 绝对地址（网关地址可配） |

第二行不是可选项：Tauri 的 WebView 从 `tauri://localhost` 加载，**相对路径会打到
应用自己身上**，而不是 gateway。所以桌面启动时必须先设 base 再渲染。

## 安全边界

暴露给 WebView 的只有 domain 级命令（`load_pending` / `upsert_pending` / …），
**没有**"读这个路径、写那个路径"这类通用文件权限——`tauri-rust-developer` skill
的原话是：把通用文件权限交给 WebView，等于把这份授权也交给了任何能在那里面执行
的代码。`capabilities/default.json` 里也只申请了 `core:default`。

CSP 在 `tauri.conf.json` 里收紧到只允许 `'self'` 加 gateway 的地址。
