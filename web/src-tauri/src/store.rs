//! 桌面端的本机持久化。
//!
//! **为什么在 Rust 侧，而不是继续用浏览器的 IndexedDB**：这些记录是"重发还是
//! 放弃"的唯一依据（`client/pending.go` 的注释写明了理由——集群不替任何人保存
//! changes），而浏览器存储会被清缓存、隐私模式、换浏览器/设备抹掉，用户甚至
//! 不知道它存在。放在这里，它落在一个用户能看见、能备份、也不随浏览器策略消失
//! 的地方。
//!
//! **为什么不让 gateway 代存**：桌面端"先记再发"的要求是"发之前必须已落盘"。
//! 如果落盘要经过 gateway，而 gateway 正是要发的那个目标，就成了循环依赖——
//! gateway 一挂既记不了也发不出去，而恰恰那时最需要留下这批数据。所以这里直接
//! 写本机文件，与后端是否可达无关。
//!
//! 三条来自 `tauri-rust-developer` skill 的硬约束：
//!   1. **原子写**：同目录 sibling temp + rename。半写的状态文件会恰好丢掉要紧的记录。
//!   2. **不靠退出时保存**：app 和机器都会崩，所以每次写入立即 fsync + rename。
//!   3. **不给 WebView 通用文件权限**：这里只有 domain 命令（load/upsert/remove），
//!      没有"读这个路径、写那个路径"。

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// 一条文档变更，与线上 `DocChange` 同形。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Change {
    pub op: String,
    pub doc_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

/// 一条在途批次。字段与前端 `pending/store.ts` 的 `PendingWrite` 逐字对应。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingWrite {
    pub id: String,
    pub knowledge_base_id: String,
    /// protojson 把 int64 编成字符串，所以这里也是字符串（空串 = 还没拿到版本号）。
    #[serde(default)]
    pub version_id: String,
    pub client_request_id: String,
    pub changes: Vec<Change>,
    pub submitted_at: String,
    #[serde(default)]
    pub settled: bool,
}

/// 本地偏好。可重建的东西，损坏时退回默认值即可。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    /// schema 版本，为将来的迁移留的（加字段时旧文件仍然能读）。
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// gateway 的地址。桌面壳里 WebView 不是同源的（`tauri://localhost`），
    /// 所以必须显式给出；客户也可能把 gateway 跑在别的端口或机器上。
    #[serde(default = "default_gateway_url")]
    pub gateway_url: String,
    #[serde(default)]
    pub selected_kb_id: Option<String>,
    #[serde(default = "default_top_k")]
    pub top_k: u32,
    #[serde(default = "default_aggregation")]
    pub aggregation: String,
    /// 文档页的「导入模式」开关。
    #[serde(default)]
    pub durable_only: bool,
}

fn default_schema_version() -> u32 {
    1
}
fn default_gateway_url() -> String {
    "http://127.0.0.1:8081".to_string()
}
fn default_top_k() -> u32 {
    5
}
fn default_aggregation() -> String {
    "AGGREGATION_METHOD_MEDIAN".to_string()
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            schema_version: default_schema_version(),
            gateway_url: default_gateway_url(),
            selected_kb_id: None,
            top_k: default_top_k(),
            aggregation: default_aggregation(),
            durable_only: false,
        }
    }
}

/// 一条操作历史。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// ISO 8601，由前端生成（避免在 Rust 侧引入时间库）。
    pub at: String,
    /// submit | resend | activate | query | discard …
    pub kind: String,
    pub knowledge_base_id: String,
    pub detail: String,
}

/// 原子地把 `bytes` 写到 `path`。
///
/// 用 `tempfile::NamedTempFile` 而不是自己拼临时文件名，理由只有一个：
/// **Windows 的 `std::fs::rename` 不能覆盖已存在的文件**（Unix 上是原子替换，
/// Windows 上直接报错）。那样 `pending.json` 第二次写入就会失败——恰恰是"更新
/// 一条记录"这个最常用的路径。`NamedTempFile::persist` 已按平台做了正确的事
/// （Unix 走 `rename`，Windows 走 `MoveFileEx(MOVEFILE_REPLACE_EXISTING)`）。
///
/// 临时文件仍在**同目录**：跨文件系统的 rename 会失败（EXDEV），那时"原子"就没了。
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let dir = path
        .parent()
        .ok_or_else(|| format!("no parent directory for {}", path.display()))?;
    fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;

    let mut tmp = tempfile::NamedTempFile::new_in(dir)
        .map_err(|e| format!("create temp file in {}: {e}", dir.display()))?;
    // 路径先取出来：persist 会消费 tmp，之后就借用不到了。
    let tmp_path = tmp.path().display().to_string();
    tmp.write_all(bytes)
        .map_err(|e| format!("write {tmp_path}: {e}"))?;
    // 先 flush 到磁盘再替换：否则崩溃后可能替换出一个空文件——
    // "原子"只保证改名这一步，不保证内容已经落盘。
    tmp.as_file()
        .sync_all()
        .map_err(|e| format!("sync {tmp_path}: {e}"))?;
    tmp.persist(path)
        .map_err(|e| format!("persist {tmp_path} -> {}: {e}", path.display()))?;
    Ok(())
}

/// 本机存储。目录由 Tauri 提供（各平台有各自的约定位置），不自己拼。
pub struct Store {
    dir: PathBuf,
}

/// 历史文件超过这个大小就重写一遍，只留最近 [`HISTORY_KEEP`] 条。
const HISTORY_MAX_BYTES: u64 = 1 << 20;
const HISTORY_KEEP: usize = 2000;

impl Store {
    pub fn new(dir: PathBuf) -> Self {
        Store { dir }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn pending_path(&self) -> PathBuf {
        self.dir.join("pending.json")
    }
    fn settings_path(&self) -> PathBuf {
        self.dir.join("settings.json")
    }
    fn history_path(&self) -> PathBuf {
        self.dir.join("history.jsonl")
    }

    // ---------------------------------------------------------------- 在途批次

    /// 读全部在途记录。
    ///
    /// **损坏时报错，不返回空**：静默当成"没有在途数据"恰恰是最危险的一种误判——
    /// 用户会以为没有任何东西悬着，于是不会去救那批其实还在文件里的数据。
    pub fn load_pending(&self) -> Result<Vec<PendingWrite>, String> {
        let p = self.pending_path();
        match fs::read(&p) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| format!("{} 已损坏，无法解析：{e}", p.display())),
            // 首次运行没有文件：那是空存储，不是错误。
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(format!("读取 {} 失败：{e}", p.display())),
        }
    }

    fn save_pending(&self, all: &[PendingWrite]) -> Result<(), String> {
        let bytes = serde_json::to_vec_pretty(all).map_err(|e| format!("序列化失败：{e}"))?;
        write_atomic(&self.pending_path(), &bytes)
    }

    pub fn upsert_pending(&self, record: PendingWrite) -> Result<(), String> {
        // 先读再写。读失败（含损坏）时**直接返回，不写**——覆盖一个损坏的文件
        // 等于把用户仅剩的那份记录也抹掉，而那份记录本来就可能是最后一次机会。
        let mut all = self.load_pending()?;
        match all.iter_mut().find(|w| w.id == record.id) {
            Some(slot) => *slot = record,
            None => all.push(record),
        }
        self.save_pending(&all)
    }

    pub fn remove_pending(&self, id: &str) -> Result<(), String> {
        let mut all = self.load_pending()?;
        all.retain(|w| w.id != id);
        self.save_pending(&all)
    }

    // ---------------------------------------------------------------- 配置

    /// 读配置。**损坏时退回默认值**，与在途记录相反：偏好是可重建的，为它挡住
    /// 整个应用不值得。
    pub fn load_settings(&self) -> Result<Settings, String> {
        let p = self.settings_path();
        match fs::read(&p) {
            // 缺字段用默认、多出来的字段忽略：加字段不会让旧文件失效。
            Ok(bytes) => Ok(serde_json::from_slice(&bytes).unwrap_or_default()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Settings::default()),
            Err(e) => Err(format!("读取 {} 失败：{e}", p.display())),
        }
    }

    pub fn save_settings(&self, settings: &Settings) -> Result<(), String> {
        let bytes = serde_json::to_vec_pretty(settings).map_err(|e| format!("序列化失败：{e}"))?;
        write_atomic(&self.settings_path(), &bytes)
    }

    // ---------------------------------------------------------------- 历史

    /// 追加一条历史。用 JSONL（一行一条）——追加是 O(1)，且半行损坏只影响那一行。
    pub fn append_history(&self, entry: &HistoryEntry) -> Result<(), String> {
        let p = self.history_path();
        if let Some(dir) = p.parent() {
            fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        }

        let mut line = serde_json::to_string(entry).map_err(|e| format!("序列化失败：{e}"))?;
        line.push('\n');

        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&p)
            .map_err(|e| format!("打开 {} 失败：{e}", p.display()))?;
        f.write_all(line.as_bytes())
            .map_err(|e| format!("写入 {} 失败：{e}", p.display()))?;
        f.sync_all()
            .map_err(|e| format!("sync {} 失败：{e}", p.display()))?;

        // 超限就重写一遍。历史是"发生过什么"，丢老条目可以接受，但不能无限长。
        if f.metadata().map(|m| m.len()).unwrap_or(0) > HISTORY_MAX_BYTES {
            drop(f);
            let kept = self.load_history(HISTORY_KEEP)?;
            let mut out = String::new();
            for e in &kept {
                out.push_str(&serde_json::to_string(e).map_err(|e| format!("序列化失败：{e}"))?);
                out.push('\n');
            }
            write_atomic(&p, out.as_bytes())?;
        }
        Ok(())
    }

    /// 读最近 `limit` 条历史（新的在前）。坏掉的行直接跳过——它是追加式文件，
    /// 一行损坏不该让整段历史读不出来。
    pub fn load_history(&self, limit: usize) -> Result<Vec<HistoryEntry>, String> {
        let p = self.history_path();
        let text = match fs::read_to_string(&p) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(format!("读取 {} 失败：{e}", p.display())),
        };

        let mut out: Vec<HistoryEntry> = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str::<HistoryEntry>(l).ok())
            .collect();
        out.reverse(); // 新的在前
        out.truncate(limit);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("tempdir");
        let s = Store::new(dir.path().to_path_buf());
        (dir, s)
    }

    fn record(id: &str, content: &str) -> PendingWrite {
        PendingWrite {
            id: id.to_string(),
            knowledge_base_id: "kb-1".to_string(),
            version_id: String::new(),
            client_request_id: id.to_string(),
            changes: vec![Change {
                op: "CHANGE_OP_UPDATE".to_string(),
                doc_id: "docs/手册 v2.txt".to_string(), // 非 ASCII + 空格
                content: Some(content.to_string()),
            }],
            submitted_at: "2026-01-01T00:00:00Z".to_string(),
            settled: false,
        }
    }

    #[test]
    fn missing_file_is_an_empty_store() {
        let (_d, s) = store();
        assert!(s.load_pending().expect("load").is_empty());
    }

    #[test]
    fn round_trips_a_record() {
        let (_d, s) = store();
        s.upsert_pending(record("a", "内容 A")).expect("upsert");

        let all = s.load_pending().expect("load");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].changes[0].content.as_deref(), Some("内容 A"));
        assert_eq!(all[0].changes[0].doc_id, "docs/手册 v2.txt");
    }

    /// 覆盖已存在的文件——**这正是当初 `std::fs::rename` 在 Windows 上做不到的那件事**。
    /// 只写一次就通过的实现会漏掉它。
    #[test]
    fn upserting_the_same_id_replaces_instead_of_appending() {
        let (_d, s) = store();
        s.upsert_pending(record("a", "第一版")).expect("first");
        s.upsert_pending(record("a", "第二版")).expect("second");

        let all = s.load_pending().expect("load");
        assert_eq!(all.len(), 1, "同一个 id 不该变成两条");
        assert_eq!(all[0].changes[0].content.as_deref(), Some("第二版"));
    }

    #[test]
    fn removes_a_record() {
        let (_d, s) = store();
        s.upsert_pending(record("a", "x")).expect("a");
        s.upsert_pending(record("b", "y")).expect("b");
        s.remove_pending("a").expect("remove");

        let ids: Vec<_> = s.load_pending().expect("load").into_iter().map(|w| w.id).collect();
        assert_eq!(ids, vec!["b".to_string()]);
    }

    /// 损坏时**报错，而不是返回空**：静默当成"没有在途数据"恰恰是最危险的误判。
    #[test]
    fn corrupt_pending_file_is_an_error_not_an_empty_store() {
        let (_d, s) = store();
        fs::write(s.pending_path(), b"{ this is not json").expect("write junk");

        assert!(s.load_pending().is_err(), "损坏必须报错");
    }

    /// 更要紧的一半：损坏时 upsert **拒绝写入**。覆盖掉一个损坏的文件，等于把用户
    /// 仅剩的那条记录也抹掉——而那份记录本来就可能是最后一次机会。
    #[test]
    fn upsert_refuses_to_overwrite_a_corrupt_file() {
        let (_d, s) = store();
        let junk = b"{ not json";
        fs::write(s.pending_path(), junk).expect("write junk");

        assert!(s.upsert_pending(record("a", "x")).is_err());
        // 原始内容必须原封不动，留给用户/运维去修。
        assert_eq!(fs::read(s.pending_path()).expect("read"), junk);
    }

    /// 偏好与在途记录相反：它可重建，所以损坏时退回默认值，而不是挡住整个应用。
    #[test]
    fn corrupt_settings_fall_back_to_defaults() {
        let (_d, s) = store();
        fs::write(s.settings_path(), b"not json at all").expect("write junk");

        let got = s.load_settings().expect("settings 损坏不该是错误");
        assert_eq!(got.gateway_url, "http://127.0.0.1:8081");
        assert_eq!(got.top_k, 5);
    }

    /// 缺字段用默认、多出来的字段忽略 —— 这样加字段不会让旧文件失效。
    #[test]
    fn settings_tolerate_missing_and_unknown_fields() {
        let (_d, s) = store();
        fs::write(
            s.settings_path(),
            br#"{"top_k": 9, "something_from_the_future": true}"#,
        )
        .expect("write");

        let got = s.load_settings().expect("settings");
        assert_eq!(got.top_k, 9);
        assert_eq!(got.gateway_url, "http://127.0.0.1:8081", "缺的字段用默认");
    }

    #[test]
    fn history_is_newest_first_and_honours_the_limit() {
        let (_d, s) = store();
        for i in 0..5 {
            s.append_history(&HistoryEntry {
                at: format!("2026-01-01T00:00:0{i}Z"),
                kind: "submit".to_string(),
                knowledge_base_id: "kb-1".to_string(),
                detail: format!("第 {i} 条"),
            })
            .expect("append");
        }

        let got = s.load_history(3).expect("load");
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].detail, "第 4 条", "新的在前");
    }

    /// 追加式文件里一行损坏，不该让整段历史读不出来。
    #[test]
    fn history_skips_corrupt_lines() {
        let (_d, s) = store();
        s.append_history(&HistoryEntry {
            at: "2026-01-01T00:00:00Z".to_string(),
            kind: "submit".to_string(),
            knowledge_base_id: "kb-1".to_string(),
            detail: "好的一行".to_string(),
        })
        .expect("append");
        {
            let mut f = fs::OpenOptions::new()
                .append(true)
                .open(s.history_path())
                .expect("open");
            f.write_all("{ 半行损坏\n".as_bytes()).expect("write");
        }

        let got = s.load_history(10).expect("load");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].detail, "好的一行");
    }

    /// 原子写失败不该留下临时文件残骸，也不该破坏原文件。
    #[test]
    fn atomic_write_leaves_no_temp_files_behind() {
        let (_d, s) = store();
        s.upsert_pending(record("a", "x")).expect("upsert");

        let leftovers: Vec<_> = fs::read_dir(s.dir())
            .expect("read dir")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.contains("tmp") || n.starts_with('.'))
            .collect();
        assert!(leftovers.is_empty(), "留下残骸：{leftovers:?}");
    }
}
