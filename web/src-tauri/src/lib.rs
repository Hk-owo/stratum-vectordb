//! Stratum 桌面控制台。
//!
//! 这个壳的存在理由只有一个：**让关键数据落在一个不会随浏览器消失的地方**。
//! 界面仍然是原来那套 React（`../src`），这里只提供本机持久化与一个窗口。
//!
//! 命令刻意都是 domain 级的（`load_pending` / `upsert_pending` / …），而不是
//! "读这个路径、写那个路径"——`tauri-rust-developer` skill 的原话是：把通用文件
//! 权限交给 WebView，等于把这份授权也交给了任何能在那里面执行的代码。

mod store;

use store::{HistoryEntry, PendingWrite, Settings, Store};
use tauri::Manager;

// ------------------------------------------------------------------ 命令

#[tauri::command]
fn load_pending(store: tauri::State<'_, Store>) -> Result<Vec<PendingWrite>, String> {
    store.load_pending()
}

#[tauri::command]
fn upsert_pending(store: tauri::State<'_, Store>, record: PendingWrite) -> Result<(), String> {
    store.upsert_pending(record)
}

#[tauri::command]
fn remove_pending(store: tauri::State<'_, Store>, id: String) -> Result<(), String> {
    store.remove_pending(&id)
}

#[tauri::command]
fn load_settings(store: tauri::State<'_, Store>) -> Result<Settings, String> {
    store.load_settings()
}

#[tauri::command]
fn save_settings(store: tauri::State<'_, Store>, settings: Settings) -> Result<(), String> {
    store.save_settings(&settings)
}

#[tauri::command]
fn append_history(store: tauri::State<'_, Store>, entry: HistoryEntry) -> Result<(), String> {
    store.append_history(&entry)
}

#[tauri::command]
fn load_history(store: tauri::State<'_, Store>, limit: usize) -> Result<Vec<HistoryEntry>, String> {
    store.load_history(limit)
}

/// 数据目录的路径。给界面用的：让人能看见自己的记录存在哪，也就能备份它——
/// 顺带是排查"数据到底丢没丢"的第一步。
#[tauri::command]
fn store_dir(store: tauri::State<'_, Store>) -> String {
    store.dir().display().to_string()
}

// ------------------------------------------------------------------ 启动

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(
            tauri_plugin_log::Builder::default()
                .level(log::LevelFilter::Info)
                .build(),
        )
        .setup(|app| {
            // 目录交给 Tauri：各平台有各自的约定位置（Linux 是 XDG data dir，
            // macOS 是 Application Support，Windows 是 AppData）。
            let dir = app.path().app_data_dir()?;
            app.manage(Store::new(dir));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            load_pending,
            upsert_pending,
            remove_pending,
            load_settings,
            save_settings,
            append_history,
            load_history,
            store_dir,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
