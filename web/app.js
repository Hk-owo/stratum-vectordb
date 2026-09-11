'use strict';

/* ============================================================
   Stratum 控制台 — 前端逻辑
   API 接口与原实现完全一致：
     GET  /api/health
     GET  /api/system-status
     GET  /api/knowledge-bases
     POST /api/knowledge-bases
     GET  /api/knowledge-bases/{id}
     POST /api/knowledge-bases/delete
     GET  /api/knowledge-bases/{id}/versions
     POST /api/knowledge-bases/{id}/versions
     POST /api/knowledge-bases/{id}/rollback
     POST /api/knowledge-bases/{id}/delete-version
     POST /api/knowledge-bases/{id}/rebuild
     POST /api/knowledge-bases/{id}/warmup
     POST /api/query
   ============================================================ */

const API = '/api';

const INDEX_STATUS = {
  INDEX_STATUS_PENDING: { label: 'PENDING', cls: 'yellow' },
  INDEX_STATUS_READY: { label: 'READY', cls: 'green' },
  INDEX_STATUS_FAILED: { label: 'FAILED', cls: 'red' },
};
const HEALTH = {
  HEALTH_STATUS_HEALTHY: { label: 'HEALTHY', cls: 'green' },
  HEALTH_STATUS_DEGRADED: { label: 'DEGRADED', cls: 'yellow' },
  HEALTH_STATUS_UNHEALTHY: { label: 'UNHEALTHY', cls: 'red' },
};
const KB_STATUS = {
  KB_STATUS_ACTIVE: { label: 'ACTIVE', cls: 'green' },
  KB_STATUS_DELETING: { label: 'DELETING', cls: 'yellow' },
  KB_STATUS_DELETE_FAILED: { label: 'DELETE_FAILED', cls: 'red' },
};
const INDEX_TYPE = { INDEX_TYPE_HNSW: 'HNSW', INDEX_TYPE_IVF: 'IVF', INDEX_TYPE_FLAT: 'FLAT' };
const SIMILARITY = {
  SIMILARITY_COSINE: 'COSINE',
  SIMILARITY_EUCLIDEAN: 'EUCLIDEAN',
  SIMILARITY_INNER_PRODUCT: 'INNER_PRODUCT',
};

let currentKB = null;     // { id }
let currentKBMeta = null; // KnowledgeBaseInfo
let currentVersions = []; // 当前 KB 的版本列表（protojson：int64 字段是字符串）
let pendingPollTimer = null;
let warmupWatchVersion = null; // 预热后轮询完成的版本；离开 PENDING 时 toast 结果

const $ = (id) => document.getElementById(id);

// ---------- API ----------
async function api(path, opts = {}) {
  const res = await fetch(API + path, {
    method: opts.method || 'GET',
    headers: opts.body !== undefined ? { 'Content-Type': 'application/json' } : {},
    body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
  });
  let data = {};
  try { data = await res.json(); } catch (_) { /* non-JSON */ }
  if (!res.ok) {
    const err = new Error(data.error || res.statusText);
    err.grpcCode = data.grpc_code;
    throw err;
  }
  return data;
}

// ---------- 工具 ----------
function toast(msg) {
  const t = $('toast');
  t.textContent = msg;
  t.classList.remove('hidden');
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => t.classList.add('hidden'), 3000);
}

// ---------- 通用确认弹窗 ----------
let confirmCallback = null;

function showConfirm(title, message, opts = {}) {
  $('confirm-title').textContent = title;
  $('confirm-message').textContent = message;
  // 可选的内联附加内容（例如删除模式单选框）；不传时整体隐藏。
  const extra = $('confirm-extra');
  extra.innerHTML = opts.extraHTML || '';
  extra.classList.toggle('hidden', !opts.extraHTML);
  const ok = $('confirm-ok');
  ok.textContent = opts.okText || '确认';
  // 危险操作用红色按钮，普通操作用主题色按钮
  ok.className = 'btn ' + (opts.danger === false ? 'btn-primary' : 'btn-danger');
  confirmCallback = opts.onConfirm || null;
  $('confirm-modal').classList.remove('hidden');
  ok.focus();
}

function closeConfirm() {
  $('confirm-modal').classList.add('hidden');
  confirmCallback = null;
}

$('confirm-cancel').addEventListener('click', closeConfirm);
$('confirm-ok').addEventListener('click', () => {
  const cb = confirmCallback;
  closeConfirm();
  if (cb) cb();
});
$('confirm-modal').addEventListener('click', (e) => {
  if (e.target === $('confirm-modal')) closeConfirm(); // 点击遮罩关闭
});
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && !$('confirm-modal').classList.contains('hidden')) closeConfirm();
});

function showBanner(msg) {
  $('error-banner-text').textContent = msg;
  $('error-banner').classList.remove('hidden');
}
function hideBanner() {
  $('error-banner').classList.add('hidden');
}
$('error-banner-close').addEventListener('click', () => {
  bannerDismissed = true; // 用户手动关闭后，恢复健康前不再自动弹出
  hideBanner();
});

function badge(cls, label) {
  return `<span class="badge badge-${cls}"><span class="dot"></span>${label}</span>`;
}

function emptyList(el, msg = '无') {
  el.innerHTML = `<li class="empty">${msg}</li>`;
}

function fmtBytes(n) {
  if (n == null) return '—';
  const units = ['B', 'KB', 'MB', 'GB'];
  let i = 0, v = n;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return v.toFixed(1) + ' ' + units[i];
}

function fmtTime(ts) {
  if (!ts) return '—';
  return new Date(ts * 1000).toLocaleString();
}

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// ---------- 导航 ----------
document.querySelectorAll('.nav-item').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.nav-item').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    document.querySelectorAll('.page').forEach(p => p.classList.add('hidden'));
    $('page-' + btn.dataset.page).classList.remove('hidden');
  });
});

$('refresh-btn').addEventListener('click', () => {
  const btn = $('refresh-btn');
  btn.classList.add('spinning');
  setTimeout(() => btn.classList.remove('spinning'), 650);
  pollHealth();
  pollSystemStatus();
  renderKBList();
  pollOps();
  if (currentKB) loadVersions();
});

// ---------- 健康轮询 ----------
// 数据库连接状态跟踪：仅在“健康→异常”跃迁时提示一次；持续异常不反复
// 触发；用户手动关闭后不再弹出（数据库恢复健康后重置，下次断连可再提示）。
let healthWasUp = true;      // 上一轮轮询是否正常
let bannerDismissed = false; // 用户是否手动关闭过错误横幅

function setHealthBadge(status) {
  const h = HEALTH[status] || { label: status || 'UNKNOWN', cls: 'gray' };
  $('health-badge').className = `badge badge-${h.cls}`;
  $('health-text').textContent = h.label;
}

// 把连接失败转成友好文案：数据库未启动是正常运维场景（可在「运维」页
// 启动服务或修改参数），不把底层错误堆给用户。
function friendlyUnavailable(e) {
  const m = (e && e.message) || '';
  if (/unavailable|refused|connect|dial/i.test(m)) {
    return '数据库未连接（可能已停止）——可在「运维」页启动服务或修改参数（此提示只出现一次）';
  }
  return '无法连接服务：' + m;
}

// setHealthUI 统一更新健康状态 UI（顶栏徽标 + 总览页大字/详情/卡片边框）。
// 成功与失败分支都走这里，避免两处不一致（例如断连时只有顶栏变红而总览页
// 仍停留在最后一次成功的状态）。
function setHealthUI(status, details) {
  const h = HEALTH[status] || { label: status || 'UNKNOWN', cls: 'gray' };
  setHealthBadge(status);
  $('health-details').textContent = details;
  $('health-status').textContent = h.label;
  $('health-details-big').textContent = details;
  $('health-card').style.borderTop = '3px solid ' +
    ({ green: 'var(--green)', yellow: 'var(--yellow)', red: 'var(--red)' }[h.cls] || 'var(--gray)');
}

async function pollHealth() {
  try {
    const h = await api('/health');
    setHealthUI(h.status, h.details || 'ok');
    healthWasUp = true;
    bannerDismissed = false; // 恢复健康后重置：下次断连可再次提示
    hideBanner();
  } catch (e) {
    setHealthUI('HEALTH_STATUS_UNHEALTHY', friendlyUnavailable(e));
    if (healthWasUp && !bannerDismissed) {
      showBanner(friendlyUnavailable(e));
    }
    healthWasUp = false;
  } finally {
    $('last-refresh').textContent = '上次刷新 ' + new Date().toLocaleTimeString();
  }
}

async function pollSystemStatus() {
  try {
    const s = await api('/system-status');
    const ru = s.resource_usage || {};
    $('res-loaded-index').textContent = ru.loaded_index_count ?? '—';
    $('res-chunk-bytes').textContent = fmtBytes(ru.chunk_store_bytes);
    $('res-doc-bytes').textContent = fmtBytes(ru.doc_store_bytes);

    renderStuck(s.stuck_versions || []);
    renderDeletingVersions(s.deleting_versions || []);
    renderDeleteFailed(s.delete_failed_kbs || []);
    renderWAL(s.wal_alerts || []);
  } catch (e) {
    // 非关键，静默
  }
}

function renderStuck(list) {
  const el = $('stuck-versions');
  if (!list.length) return emptyList(el);
  el.innerHTML = list.map(v =>
    `<li>${v.kb_id} / v${v.version_id} ${badge((INDEX_STATUS[v.index_status] || {}).cls || 'gray', (INDEX_STATUS[v.index_status] || {}).label || v.index_status)}
     <span class="muted">${fmtTime(v.updated_at)}</span></li>`).join('');
}
function renderDeletingVersions(list) {
  const el = $('deleting-versions');
  if (!list.length) return emptyList(el);
  el.innerHTML = list.map(v =>
    `<li>${v.kb_id} / v${v.version_id} ${badge('yellow', '删除中')}
     <span class="muted">${fmtTime(v.updated_at)}</span></li>`).join('');
}
function renderDeleteFailed(list) {
  const el = $('delete-failed');
  if (!list.length) return emptyList(el);
  el.innerHTML = list.map(id => `<li>${id}</li>`).join('');
}
function renderWAL(list) {
  const el = $('wal-alerts');
  if (!list.length) return emptyList(el);
  el.innerHTML = list.map(a => `<li>${a.description} <span class="muted">重试 ${a.retry_count}</span></li>`).join('');
}

// ---------- 新建知识库（默认收起，按需展开） ----------
function setCreatePanel(open) {
  $('create-kb-panel').classList.toggle('open', open);
  $('toggle-create-kb').classList.toggle('active', open);
}
$('toggle-create-kb').addEventListener('click', () => {
  setCreatePanel(!$('create-kb-panel').classList.contains('open'));
});
$('cancel-create-kb').addEventListener('click', () => {
  $('create-kb-form').reset();
  setCreatePanel(false);
});

// ---------- 知识库 ----------
// 知识库显示名：name 优先。若 id 由 name 派生（id = name + 后缀，如 "name-N"），
// 只额外显示后缀以区分同名库，避免 name 与 id 重复展示。
function kbDisplayName(kb) {
  const name = kb.name || '';
  const id = kb.knowledge_base_id || '';
  if (!name) return escapeHtml(id);
  if (id === name) return escapeHtml(name);
  if (id.startsWith(name)) {
    return `${escapeHtml(name)} <span class="muted">${escapeHtml(id.slice(name.length))}</span>`;
  }
  return escapeHtml(name);
}

async function renderKBList() {
  const el = $('kb-list');
  try {
    const resp = await api('/knowledge-bases');
    // 按名称排序（中文友好），名称为空时按 knowledge_base_id 兜底，保证顺序稳定不跳。
    const kbs = (resp.knowledge_bases || []).slice().sort((a, b) => {
      const na = a.name || a.knowledge_base_id;
      const nb = b.name || b.knowledge_base_id;
      return na.localeCompare(nb, 'zh');
    });
    if (!kbs.length) { emptyList(el, '暂无知识库，点击上方按钮创建'); return; }
    el.innerHTML = kbs.map(kb => {
      const st = KB_STATUS[kb.status] || { label: kb.status, cls: 'gray' };
      return `<li data-id="${kb.knowledge_base_id}" class="${currentKB && currentKB.id === kb.knowledge_base_id ? 'selected' : ''}">
        <span>${kbDisplayName(kb)} ${badge(st.cls, st.label)}</span>
      </li>`;
    }).join('');
    el.querySelectorAll('li').forEach(li => li.addEventListener('click', () => selectKB(li.dataset.id)));
    refreshDatalist(kbs);
  } catch (e) {
    el.innerHTML = `<li class="empty">加载失败：${e.message}</li>`;
  }
}

function refreshDatalist(kbs) {
  const dl = $('kb-datalist');
  dl.innerHTML = kbs.map(kb => `<option value="${kb.knowledge_base_id}">${kb.name || kb.knowledge_base_id}</option>`).join('');
}

function showKBDetail(visible) {
  $('kb-empty').classList.toggle('hidden', visible);
  $('kb-detail').classList.toggle('hidden', !visible);
}

async function selectKB(id) {
  currentKB = { id };
  renderKBList();
  showKBDetail(true);
  $('kb-detail-title').textContent = id;
  $('kb-detail-meta').textContent = '';
  try {
    const resp = await api('/knowledge-bases/' + encodeURIComponent(id));
    currentKBMeta = resp.knowledge_base;
    renderKBDetail(currentKBMeta);
  } catch (e) {
    currentKBMeta = null;
    $('kb-detail-meta').textContent = '加载元数据失败：' + e.message;
  }
  loadVersions();
}

function clearKBDetail() {
  currentKB = null;
  currentKBMeta = null;
  currentVersions = [];
  showKBDetail(false);
  $('kb-versions').innerHTML = '';
  $('parent-version-select').innerHTML = '';
  $('changes-editor').innerHTML = '';
  updateChangesEmpty();
}

function renderKBDetail(kb) {
  const st = KB_STATUS[kb.status] || { label: kb.status, cls: 'gray' };
  $('kb-detail-title').textContent = (kb.name ? kb.name + ' · ' : '') + kb.knowledge_base_id;
  $('kb-detail-meta').innerHTML =
    badge(st.cls, st.label) +
    ' 活跃版本 v' + (kb.active_version_id ?? '—') +
    ' · ' + (INDEX_TYPE[kb.index_type] || kb.index_type) +
    ' · ' + (SIMILARITY[kb.similarity] || kb.similarity);
}

async function loadVersions() {
  if (!currentKB) return;
  const el = $('kb-versions');
  el.innerHTML = '<span class="muted">加载中…</span>';
  try {
    const resp = await api('/knowledge-bases/' + encodeURIComponent(currentKB.id) + '/versions');
    const versions = (resp.versions || []).slice().sort((a, b) => a.version_id - b.version_id);
    currentVersions = versions;
    renderVersions(versions);
    renderParentSelect(versions);
    // 预热结果反馈：被预热的版本一旦离开 PENDING（READY/FAILED），toast 结果
    if (warmupWatchVersion != null) {
      const wv = versions.find(v => v.version_id === warmupWatchVersion);
      if (wv && wv.index_status !== 'INDEX_STATUS_PENDING') {
        const st = INDEX_STATUS[wv.index_status] || { label: wv.index_status };
        const ok = wv.index_status === 'INDEX_STATUS_READY';
        toast(ok ? `预热完成：v${wv.version_id} ${st.label}` : `预热失败：v${wv.version_id} ${st.label}`);
        warmupWatchVersion = null;
      }
    }
    // 有 PENDING 版本时自动跟进一次状态，READY/FAILED 后停止
    clearTimeout(pendingPollTimer);
    if (versions.some(v => v.index_status === 'INDEX_STATUS_PENDING')) {
      pendingPollTimer = setTimeout(loadVersions, 3000);
    }
  } catch (e) {
    el.innerHTML = `<span class="muted">加载失败：${e.message}</span>`;
  }
}

function renderVersions(versions) {
  const el = $('kb-versions');
  if (!versions.length) { el.innerHTML = '<span class="muted">暂无版本</span>'; return; }
  const active = currentKBMeta ? currentKBMeta.active_version_id : null;

  // 本库内序号：按全局 version_id 升序排序后的位置（1-based）。
  // version_id 全局单调递增，所以它等价于「该知识库内第 N 个版本」。
  const localNo = {};
  versions.slice().sort((a, b) => Number(a.version_id) - Number(b.version_id))
    .forEach((v, i) => { localNo[String(v.version_id)] = i + 1; });

  // 按 parent_version_id 建树，支持分叉（同一父版本多个子版本，A/B 场景）。
  // 注意：protojson 把 int64 序列化为字符串，统一 String() 比较，避免 "0" !== 0 这类坑。
  const byParent = {};
  const ids = new Set(versions.map(v => String(v.version_id)));
  for (const v of versions) {
    const p = String(v.parent_version_id ?? '0');
    (byParent[p] = byParent[p] || []).push(v);
  }
  for (const k of Object.keys(byParent)) byParent[k].sort((a, b) => Number(a.version_id) - Number(b.version_id));
  // 根节点：parent=0；孤儿节点（parent 不在本列表，历史被截断）也作为根。
  const roots = (byParent["0"] || []).concat(
    versions.filter(v => {
      const p = String(v.parent_version_id ?? '0');
      return p !== '0' && !ids.has(p);
    })
  );

  const rows = [];
  const walk = (v, depth) => {
    const st = INDEX_STATUS[v.index_status] || { label: v.index_status, cls: 'gray' };
    const actions = [];
    // 删除中：异步清理尚未完成，除刷新外无操作可用。
    if (v.deleting) {
      actions.push(`<button class="btn btn-ghost" disabled title="删除清理进行中…">删除中…</button>`);
    } else {
      if (v.index_status === 'INDEX_STATUS_READY') {
        actions.push(`<button class="btn btn-ghost" data-act="rollback" data-v="${v.version_id}">回滚</button>`);
        actions.push(`<button class="btn btn-ghost" data-act="warmup" data-v="${v.version_id}">预热</button>`);
      }
      if (v.index_status === 'INDEX_STATUS_FAILED') {
        actions.push(`<button class="btn btn-ghost" data-act="rebuild" data-v="${v.version_id}">重建索引</button>`);
      }
      // 删除版本：活跃版本不可删除（后端也会拒绝），禁用并提示；其余版本
      // 打开删除弹窗，可显式选择删除范围（子树 / 仅自身 / 前置版本）。
      if (v.version_id === active) {
        actions.push(`<button class="btn btn-ghost" disabled title="活跃版本不可删除，请先回滚到其他版本">删除</button>`);
      } else {
        actions.push(`<button class="btn btn-danger" data-act="delete" data-v="${v.version_id}">删除</button>`);
      }
      // 设为基底：删除该版本的全部前置版本，使其成为版本链新的根。
      // 已是根的版本（parent_version_id = 0）没有前置可删，不出现该按钮。
      if (String(v.parent_version_id ?? '0') !== '0') {
        actions.push(`<button class="btn btn-ghost" data-act="setbase" data-v="${v.version_id}">设为基底</button>`);
      }
    }
    const activeTag = v.version_id === active ? '<span class="muted">（活跃）</span>' : '';
    const deletingTag = v.deleting ? badge('yellow', '删除中') : '';
    rows.push(`<div class="version-node ${v.version_id === active ? 'active' : ''}" style="margin-left:${depth * 26}px">
      <span class="tree-corner">${depth > 0 ? '└─' : ''}</span>
      ${badge(st.cls, st.label)}
      ${deletingTag}
      <strong>v${v.version_id}</strong>
      <span class="local-no">本库 #${localNo[String(v.version_id)]}</span>
      <span class="meta">父 v${v.parent_version_id ?? '0'} · ${fmtTime(v.created_at)} ${activeTag}</span>
      <span class="actions">${actions.join('')}</span>
    </div>`);
    (byParent[String(v.version_id)] || []).forEach(c => walk(c, depth + 1));
  };
  roots.forEach(r => walk(r, 0));

  el.innerHTML = rows.join('');

  el.querySelectorAll('button[data-act]').forEach(b => {
    b.addEventListener('click', () => {
      const vid = b.dataset.v;
      const act = b.dataset.act;
      if (act === 'rollback') rollback(vid);
      else if (act === 'rebuild') rebuild(vid);
      else if (act === 'warmup') warmup(vid);
      else if (act === 'delete') deleteVersion(vid);
      else if (act === 'setbase') setBaseVersion(vid);
    });
  });
}

function renderParentSelect(versions) {
  const sel = $('parent-version-select');
  const sorted = versions.slice().sort((a, b) => b.version_id - a.version_id);
  sel.innerHTML = sorted.map(v => `<option value="${v.version_id}">v${v.version_id}（${(INDEX_STATUS[v.index_status] || {}).label || v.index_status}）</option>`).join('');
}

async function rollback(versionId) {
  // 回滚可逆、无停机（随时可回退），无需确认弹窗，直接执行。
  try {
    await api(`/knowledge-bases/${encodeURIComponent(currentKB.id)}/rollback`, {
      method: 'POST', body: { target_version_id: Number(versionId) },
    });
    toast(`已回滚到 v${versionId}`);
    selectKB(currentKB.id);
  } catch (e) { toast('回滚失败：' + e.message); }
}

async function rebuild(versionId) {
  try {
    await api(`/knowledge-bases/${encodeURIComponent(currentKB.id)}/rebuild`, {
      method: 'POST', body: { version_id: Number(versionId) },
    });
    toast('已触发重建索引（异步）');
    warmupWatchVersion = null; // 重建结果由 PENDING 徽章体现，不再当作预热完成上报
    setTimeout(loadVersions, 1000);
  } catch (e) { toast('重建失败：' + e.message); }
}

async function warmup(versionId) {
  try {
    await api(`/knowledge-bases/${encodeURIComponent(currentKB.id)}/warmup`, {
      method: 'POST', body: { version_id: Number(versionId) },
    });
    toast('已触发预热（异步）');
    warmupWatchVersion = Number(versionId);
    setTimeout(loadVersions, 1000);
  } catch (e) { toast('预热失败：' + e.message); }
}

// findVersion 在当前版本列表里按 version_id 查一个版本。protojson 把 int64
// 序列化为字符串，统一用 String() 比较，避免 "3" !== 3 这类坑。
function findVersion(versionId) {
  return currentVersions.find(v => String(v.version_id) === String(versionId)) || null;
}

// ancestorDeleteImpact 计算「把 versionId 设为基底」会连带删除的版本集合：
// 它的全部前置版本，以及这些前置版本上挂着的其它分支（旁支）。保留集是目标
// 版本及其子树。算法与后端 ANCESTORS 模式（internal/raft/state_machine.go
// 的 versionDeleteTargets）保持一致，仅用于删除前的确认提示。
function ancestorDeleteImpact(versions, versionId) {
  const byId = new Map(versions.map(v => [String(v.version_id), v]));
  const childrenOf = new Map();
  for (const v of versions) {
    const p = String(v.parent_version_id ?? '0');
    if (!childrenOf.has(p)) childrenOf.set(p, []);
    childrenOf.get(p).push(String(v.version_id));
  }
  const target = byId.get(String(versionId));
  if (!target) return [];

  // 祖先链（遇到根或断链即止）
  const ancestors = [];
  const seenAncestor = new Set([String(versionId)]);
  let cur = target;
  for (;;) {
    const p = String(cur.parent_version_id ?? '0');
    if (p === '0' || !byId.has(p) || seenAncestor.has(p)) break;
    seenAncestor.add(p);
    ancestors.push(p);
    cur = byId.get(p);
  }
  if (!ancestors.length) return [];

  // 保留集：目标版本及其全部后代
  const keep = new Set();
  const stack = [String(versionId)];
  while (stack.length) {
    const id = stack.pop();
    if (keep.has(id)) continue;
    keep.add(id);
    for (const c of (childrenOf.get(id) || [])) stack.push(c);
  }

  // 受影响：祖先子树中不属于保留集的版本
  const affected = new Set();
  const pending = ancestors.slice();
  const visited = new Set();
  while (pending.length) {
    const id = pending.pop();
    if (visited.has(id)) continue;
    visited.add(id);
    if (keep.has(id)) continue;
    affected.add(id);
    for (const c of (childrenOf.get(id) || [])) pending.push(c);
  }
  return [...affected].map(Number).sort((a, b) => a - b);
}

// subtreeIDs 返回 versionId 自身及其全部后代（含分叉），对应后端
// collectVersionSubtree，用于 SUBTREE 模式的波及提示。
function subtreeIDs(versions, versionId) {
  const childrenOf = new Map();
  for (const v of versions) {
    const p = String(v.parent_version_id ?? '0');
    if (!childrenOf.has(p)) childrenOf.set(p, []);
    childrenOf.get(p).push(String(v.version_id));
  }
  const out = [];
  const seen = new Set();
  const stack = [String(versionId)];
  while (stack.length) {
    const id = stack.pop();
    if (seen.has(id)) continue;
    seen.add(id);
    out.push(Number(id));
    for (const c of (childrenOf.get(id) || [])) stack.push(c);
  }
  return out;
}

// activeVersionID 返回当前活跃版本的字符串 ID（protojson 的 int64 为字符串）；
// 无活跃版本信息时返回 null。
function activeVersionID() {
  return currentKBMeta && currentKBMeta.active_version_id != null
    ? String(currentKBMeta.active_version_id)
    : null;
}

// formatVersionList 把版本 ID 列表格式化为易读文本，过长时只列前 8 个并给出
// 总数，避免 toast / 弹窗被深层子树的长列表撑爆。
function formatVersionList(ids) {
  const shown = ids.slice(0, 8).map(id => 'v' + id).join('、');
  return ids.length > 8 ? `${shown} 等共 ${ids.length} 个版本` : shown;
}

async function deleteVersion(versionId) {
  // 删除不可撤销，且现在有「删哪些版本」的语义选择，必须二次确认。
  const v = findVersion(versionId);
  const target = `v${versionId}`;
  const parentId = v ? String(v.parent_version_id ?? '0') : '0';
  const parentVersion = parentId === '0' ? null : findVersion(parentId);
  // 父版本不可用（不存在或正在删除中）时子版本会成为新的根——与后端
  // spliceParent 的判定保持一致。
  const spliceHint = (parentVersion && !parentVersion.deleting)
    ? `删除后 ${target} 的子版本会自动改挂到它的父版本 v${parentId} 上，分支结构保留。`
    : `删除后 ${target} 的子版本会成为新的根版本。`;

  const activeId = activeVersionID();
  const impact = ancestorDeleteImpact(currentVersions, versionId);
  const impactHasActive = activeId != null && impact.some(id => String(id) === activeId);
  const baseHint = impact.length
    ? `删除它的全部前置版本：${formatVersionList(impact)}（含这些版本上挂着的其它分支）${impactHasActive ? `；其中包含活跃版本 v${activeId}，后端会拒绝该操作` : ''}。`
    : `${target} 已经是版本链的根，没有前置版本可删。`;

  const subtree = subtreeIDs(currentVersions, versionId);
  const subtreeHasActive = activeId != null && subtree.some(id => String(id) === activeId);
  const subtreeHint = `整段截断：该版本之后派生出的版本一并删除${subtreeHasActive ? `；其中包含活跃版本 v${activeId}，后端会拒绝该操作` : ''}。`;

  const modes = [
    { value: 'VERSION_DELETE_MODE_SUBTREE', label: `删除 ${target} 及其所有子版本`, hint: subtreeHint },
    { value: 'VERSION_DELETE_MODE_SINGLE', label: `仅删除 ${target}`, hint: spliceHint },
    { value: 'VERSION_DELETE_MODE_ANCESTORS', label: `设为基底：删除 ${target} 的全部前置版本`, hint: baseHint },
  ];
  const extraHTML = modes.map((m, i) => `
    <label class="mode-option">
      <input type="radio" name="delete-mode" value="${m.value}" ${i === 0 ? 'checked' : ''}>
      <span class="mode-text"><strong>${m.label}</strong><span class="muted">${m.hint}</span></span>
    </label>`).join('');

  showConfirm(`删除版本 ${target}？`, '删除不可撤销：被删版本将无法再被查询或回滚。请选择删除范围。', {
    okText: '删除',
    extraHTML,
    onConfirm: async () => {
      const picked = document.querySelector('input[name="delete-mode"]:checked');
      const mode = picked ? picked.value : 'VERSION_DELETE_MODE_SUBTREE';
      // 与「设为基底」按钮一致的提前拦截：活跃版本不可删，别让用户白提交一次。
      if (mode === 'VERSION_DELETE_MODE_ANCESTORS' && impactHasActive) {
        toast(`无法把 ${target} 设为基底：前置版本中包含活跃版本 v${activeId}，请先回滚到其它版本`);
        return;
      }
      if (mode === 'VERSION_DELETE_MODE_SUBTREE' && subtreeHasActive) {
        toast(`无法删除 ${target} 的整段子树：其中包含活跃版本 v${activeId}，请先回滚到其它版本`);
        return;
      }
      try {
        const resp = await api(`/knowledge-bases/${encodeURIComponent(currentKB.id)}/delete-version`, {
          method: 'POST', body: { version_id: Number(versionId), mode },
        });
        const ids = (resp.deleted_version_ids || []).map(String);
        toast(ids.length
          ? `已标记删除：${formatVersionList(ids)}（异步清理，完成后将从列表消失）`
          : '没有需要删除的版本（该版本已是基底）');
        selectKB(currentKB.id);
      } catch (e) { toast('删除失败：' + e.message); }
    },
  });
}

// setBaseVersion 把某个版本设为知识库基底：删除它的全部前置版本，使其成为
// 版本链新的根（后端 ANCESTORS 模式）。
async function setBaseVersion(versionId) {
  const target = `v${versionId}`;
  const impact = ancestorDeleteImpact(currentVersions, versionId);
  if (!impact.length) {
    toast(`${target} 已经是版本链的根，无需删除前置版本`);
    return;
  }
  // 活跃版本不可删除：与其等后端拒绝，不如在这里提前拦截（后端仍会兜底）。
  const activeId = activeVersionID();
  if (activeId != null && impact.some(id => String(id) === activeId)) {
    toast(`无法把 ${target} 设为基底：前置版本中包含活跃版本 v${activeId}，请先回滚到其它版本`);
    return;
  }
  showConfirm(
    `将 ${target} 设为知识库基底？`,
    `将删除 ${target} 的全部前置版本：${formatVersionList(impact)}（含这些版本上挂着的其它分支），此操作不可撤销。`,
    {
      okText: '设为基底',
      onConfirm: async () => {
        try {
          const resp = await api(`/knowledge-bases/${encodeURIComponent(currentKB.id)}/delete-version`, {
            method: 'POST', body: { version_id: Number(versionId), mode: 'VERSION_DELETE_MODE_ANCESTORS' },
          });
          const ids = (resp.deleted_version_ids || []).map(String);
          toast(ids.length
            ? `已把 ${target} 设为基底，正在删除前置版本：${formatVersionList(ids)}`
            : `${target} 已经是基底`);
          selectKB(currentKB.id);
        } catch (e) { toast('设为基底失败：' + e.message); }
      },
    }
  );
}

$('create-kb-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = e.target;
  const body = {
    name: f.name.value.trim(),
    chunk_window_size: Number(f.chunk_window_size.value) || 512,
    chunk_overlap_size: Number(f.chunk_overlap_size.value) || 64,
    index_type: f.index_type.value,
    similarity: f.similarity.value,
    embed_config: { service_addr: f.service_addr.value.trim(), model_id: f.model_id.value.trim() },
  };
  try {
    const resp = await api('/knowledge-bases', { method: 'POST', body });
    toast(`已创建 ${resp.knowledge_base_id}，初始版本 v${resp.initial_version_id}`);
    f.reset();
    setCreatePanel(false);   // 创建成功后表单收起消失
    await renderKBList();
    selectKB(resp.knowledge_base_id);
  } catch (err) { toast('创建失败：' + err.message); }
});

$('delete-kb-btn').addEventListener('click', () => {
  if (!currentKB) return;
  // 删除不可逆（异步清理所有存储数据），保留确认，改用自定义弹窗。
  showConfirm('删除知识库', `确定删除知识库 ${currentKB.id} 吗？将异步清理所有存储数据，不可撤销。`, {
    okText: '删除',
    onConfirm: async () => {
      try {
        await api('/knowledge-bases/delete', { method: 'POST', body: { knowledge_base_id: currentKB.id } });
        toast('已标记删除（异步清理）');
        clearKBDetail();   // 详情消失，回到空态
        renderKBList();
      } catch (e) { toast('删除失败：' + e.message); }
    },
  });
});

// ---------- 创建版本 / 变更编辑器 ----------
function updateChangesEmpty() {
  const has = document.querySelectorAll('#changes-editor .change-row').length > 0;
  $('changes-empty').classList.toggle('hidden', has);
}

function addChangeRow(op = 'CHANGE_OP_ADD', docId = '', content = '') {
  const row = document.createElement('div');
  row.className = 'change-row';
  row.innerHTML = `
    <select class="op">
      <option value="CHANGE_OP_ADD">ADD</option>
      <option value="CHANGE_OP_DELETE">DELETE</option>
      <option value="CHANGE_OP_UPDATE">UPDATE</option>
    </select>
    <input class="doc-id" placeholder="doc_id" value="${docId}">
    <textarea class="content" placeholder="content（ADD/UPDATE 必填，DELETE 无需）">${content}</textarea>
    <button type="button" class="btn btn-ghost remove">移除</button>
  `;
  row.querySelector('select.op').value = op;
  row.querySelector('.remove').addEventListener('click', () => { row.remove(); updateChangesEmpty(); });
  row.querySelector('.op').addEventListener('change', function () {
    const isDelete = this.value === 'CHANGE_OP_DELETE';
    row.querySelector('.content').disabled = isDelete;
    row.querySelector('.content').placeholder = isDelete
      ? 'DELETE 无需内容'
      : 'content（ADD/UPDATE 必填）';
  });
  $('changes-editor').appendChild(row);
  updateChangesEmpty();
  row.querySelector('.doc-id').focus();
}

$('add-change-btn').addEventListener('click', () => addChangeRow());

$('create-version-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  if (!currentKB) { toast('请先选择知识库'); return; }
  const changes = [];
  document.querySelectorAll('#changes-editor .change-row').forEach(row => {
    const op = row.querySelector('.op').value;
    const docId = row.querySelector('.doc-id').value.trim();
    if (!docId) return;
    const change = { op, doc_id: docId };
    if (op !== 'CHANGE_OP_DELETE') change.content = row.querySelector('.content').value;
    changes.push(change);
  });
  if (!changes.length) { toast('至少添加一条文档变更'); return; }
  const parent = Number(e.target.parent_version_id.value);
  try {
    const resp = await api(`/knowledge-bases/${encodeURIComponent(currentKB.id)}/versions`, {
      method: 'POST', body: { parent_version_id: parent, changes },
    });
    toast(`已创建版本 v${resp.version_id}（PENDING，索引异步构建中）`);
    $('changes-editor').innerHTML = '';
    updateChangesEmpty();
    loadVersions();
    setTimeout(loadVersions, 3000);
  } catch (err) { toast('创建版本失败：' + err.message); }
});

// ---------- 检索 ----------
$('query-version-mode').addEventListener('change', function () {
  const specific = this.value === 'specific';
  $('query-version-id-field').classList.toggle('open', specific);
  if (!specific) document.querySelector('#query-form input[name="version_id"]').value = '';
});

function showQueryEmpty(visible) {
  $('query-empty').classList.toggle('hidden', !visible);
  $('query-results').classList.toggle('hidden', visible);
}

$('query-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = e.target;
  let vector;
  try {
    vector = JSON.parse(f.vector.value.trim());
    if (!Array.isArray(vector) || !vector.length || vector.some(x => typeof x !== 'number')) throw new Error('非法向量');
  } catch (err) { toast('查询向量需为数字数组 JSON，如 [0.1, 0.2]'); return; }

  const body = {
    knowledge_base_id: f.knowledge_base_id.value.trim(),
    vector,
    top_k: Number(f.top_k.value) || 10,
    aggregation: f.aggregation.value,
  };
  if (f.version_id.value.trim() !== '') body.version_id = Number(f.version_id.value);
  if (f.threshold.value.trim() !== '') body.threshold = Number(f.threshold.value);

  const resultsEl = $('query-results');
  const submitBtn = $('query-submit');
  showQueryEmpty(false);
  resultsEl.innerHTML = '<span class="muted">检索中…</span>';
  submitBtn.disabled = true;
  try {
    const resp = await api('/query', { method: 'POST', body });
    $('query-version').textContent = resp.version_id ? '· 命中版本 v' + resp.version_id : '';
    const results = resp.results || [];
    if (!results.length) {
      resultsEl.innerHTML = '';
      showQueryEmpty(true);
      $('query-empty .empty-hint').textContent = '无结果（可能低于阈值或文档不足 top_k）';
      return;
    }
    const maxScore = Math.max(...results.map(r => r.score));
    resultsEl.innerHTML = results.map((r, i) => {
      const pct = maxScore > 0 ? Math.max(0, Math.min(100, Math.round((r.score / maxScore) * 100))) : 0;
      return `<div class="result-card">
        <div class="head"><span>#${i + 1} ${r.doc_id}</span><span class="score">${r.score.toFixed(4)}</span></div>
        <div class="score-bar"><div class="score-bar-fill" style="width:${pct}%"></div></div>
        <div class="content">${escapeHtml(r.content || '')}</div>
      </div>`;
    }).join('');
  } catch (err) {
    resultsEl.innerHTML = `<span class="muted">检索失败：${err.message}</span>`;
  } finally {
    submitBtn.disabled = false;
  }
});

// ---------- 启动 ----------
renderKBList();
updateChangesEmpty();
pollHealth();
pollSystemStatus();
setInterval(pollHealth, 5000);
setInterval(pollSystemStatus, 5000);


// ============================================================
// 运维（Docker 集群控制台）
// 集群参数是集群级统一配置（节点数/端口/网络/镜像等），不做单节点差异化
// 修改：修改参数后需重建整个集群。所有请求走 /ops/docker/*，由网关转调
// scripts/docker-cluster.sh 执行。
// ============================================================
const DK = { status: null, config: null, logNode: 1, busy: false };

async function opsApi(path, opts = {}) {
  const res = await fetch('/ops' + path, {
    method: opts.method || 'GET',
    headers: opts.body !== undefined ? { 'Content-Type': 'application/json' } : {},
    body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
  });
  let data = {};
  try { data = await res.json(); } catch (_) { /* non-JSON */ }
  if (!res.ok) {
    throw new Error(data.error || res.statusText);
  }
  return data;
}

// ---------- 集群状态 ----------

async function dkPoll() {
  try {
    const st = await opsApi('/docker/status');
    DK.status = st;
    renderDkOverview(st);
    renderDkNodes(st);
    fillDkLogSelect(st);
  } catch (e) {
    const msg = `<span class="muted">加载失败：${escapeHtml(e.message)}</span>`;
    $('dk-overview').innerHTML = msg;
    $('dk-nodes').innerHTML = msg;
  }
}

function dkBadge(v) {
  const map = { running: 'green', healthy: 'green', starting: 'yellow', unhealthy: 'red', exited: 'gray', absent: 'gray', dead: 'red', created: 'gray' };
  const color = map[v] || 'gray';
  const label = { running: '运行中', healthy: '健康', starting: '启动中', unhealthy: '异常', exited: '已退出', absent: '不存在', dead: '已死', created: '已创建' }[v] || v;
  return badge(color, label);
}

function renderDkOverview(st) {
  const nodes = st.nodes || [];
  const running = nodes.filter(n => n.status === 'running').length;
  const healthy = nodes.filter(n => n.health === 'healthy').length;
  const leader = nodes.find(n => n.leader);
  $('dk-overview').innerHTML =
    `网络 <b>${escapeHtml(st.network)}</b> · 节点 <b>${st.count}</b>（运行 ${running} / 健康 ${healthy}）` +
    ` · 基础端口 <b>${st.base_port}</b> · 镜像 <b>${escapeHtml(st.image)}</b>` +
    ` · leader <b>${leader ? escapeHtml(leader.name) : '—'}</b>`;
}

function renderDkNodes(st) {
  const el = $('dk-nodes');
  const nodes = st.nodes || [];
  if (!nodes.length) { el.innerHTML = '<span class="muted">集群配置为空</span>'; return; }
  const rows = nodes.map(n => {
    const running = n.status === 'running';
    const act = running
      ? `<button class="btn btn-ghost" data-act="stop" data-id="${n.id}">停止</button>
         <button class="btn btn-ghost" data-act="restart" data-id="${n.id}">重启</button>`
      : `<button class="btn btn-ghost" data-act="start" data-id="${n.id}">启动</button>`;
    return `<tr>
      <td>node ${n.id} <span class="muted">${escapeHtml(n.name)}</span></td>
      <td>${dkBadge(n.status)}</td>
      <td>${dkBadge(n.health)}</td>
      <td class="mono">:${n.grpc_port}</td>
      <td>${n.leader ? '★' : ''}</td>
      <td>${act} <button class="btn btn-ghost" data-act="logs" data-id="${n.id}">日志</button></td>
    </tr>`;
  }).join('');
  el.innerHTML = `<table class="dk-table">
    <thead><tr><th>节点</th><th>状态</th><th>健康</th><th>gRPC 端口</th><th>leader</th><th>操作</th></tr></thead>
    <tbody>${rows}</tbody></table>`;
  el.querySelectorAll('button[data-act]').forEach(b => {
    b.addEventListener('click', () => {
      const id = Number(b.dataset.id);
      if (b.dataset.act === 'logs') {
        DK.logNode = id;
        fillDkLogSelect(DK.status);
        dkLoadLogs();
      } else {
        dkNodeControl(id, b.dataset.act);
      }
    });
  });
}

// ---------- 集群整体操作 ----------

async function dkClusterAction(action, force, msg) {
  if (DK.busy) { toast('上一步操作还在执行中'); return; }
  DK.busy = true;
  try {
    toast('正在执行：' + msg + '（docker 操作可能耗时数十秒）');
    await opsApi('/docker/' + action, { method: 'POST', body: force ? { force: true } : {} });
    toast(msg + ' 完成');
    setTimeout(dkPoll, 1500);
  } catch (e) {
    toast(msg + ' 失败：' + e.message);
  } finally {
    DK.busy = false;
  }
}

$('dk-up').addEventListener('click', () => dkClusterAction('up', false, '启动集群'));
$('dk-rebuild').addEventListener('click', () => dkClusterAction('up', true, '重建集群'));
$('dk-down').addEventListener('click', () => dkClusterAction('down', false, '停止集群'));
$('dk-clean').addEventListener('click', () => dkClusterAction('clean', false, '清理集群'));

// ---------- 单节点操作 ----------

async function dkNodeControl(id, action) {
  if (DK.busy) { toast('上一步操作还在执行中'); return; }
  DK.busy = true;
  try {
    await opsApi(`/docker/nodes/${id}/${action}`, { method: 'POST' });
    toast(`node ${id} 已${action === 'start' ? '启动' : action === 'stop' ? '停止' : '重启'}`);
    setTimeout(dkPoll, 1500);
  } catch (e) {
    toast(`node ${id} 操作失败：` + e.message);
  } finally {
    DK.busy = false;
  }
}

// ---------- 集群参数（集群级统一配置） ----------

async function dkConfigLoad() {
  try {
    DK.config = await opsApi('/docker/config');
    dkConfigFill(DK.config);
  } catch (e) {
    toast('加载集群参数失败：' + e.message);
  }
}

function dkConfigFill(cfg) {
  const set = (id, v) => { const el = $(id); if (el) el.value = (v == null ? '' : v); };
  set('dk-cfg-nodes', cfg.nodes);
  set('dk-cfg-baseport', cfg.base_port);
  set('dk-cfg-network', cfg.network);
  set('dk-cfg-image', cfg.image);
  set('dk-cfg-prefix', cfg.container_prefix);
  $('dk-cfg-embed').checked = !!cfg.with_embed;
}

function dkConfigRead() {
  return {
    enabled: true,
    nodes: opsNum($('dk-cfg-nodes').value) || 3,
    base_port: opsNum($('dk-cfg-baseport').value) || 17000,
    network: $('dk-cfg-network').value.trim(),
    image: $('dk-cfg-image').value.trim(),
    container_prefix: $('dk-cfg-prefix').value.trim(),
    with_embed: $('dk-cfg-embed').checked,
  };
}

function opsNum(v) {
  if (v === '' || v == null) return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

async function dkConfigSave(rebuild) {
  const body = dkConfigRead();
  if (!body.network || !body.image || !body.container_prefix) {
    toast('网络 / 镜像 / 容器前缀不能为空');
    return;
  }
  try {
    const resp = await opsApi('/docker/config', { method: 'PUT', body });
    if (rebuild) {
      await dkClusterAction('up', true, '按新参数重建集群');
    } else {
      toast('集群参数已保存' + (resp.note ? `：${resp.note}` : ''));
      dkConfigLoad();
    }
  } catch (e) {
    toast('保存集群参数失败：' + e.message);
  }
}

$('dk-config-form').addEventListener('submit', (e) => { e.preventDefault(); dkConfigSave(false); });
$('dk-config-apply').addEventListener('click', () => dkConfigSave(true));
$('dk-config-reset').addEventListener('click', () => { if (DK.config) dkConfigFill(DK.config); });

// ---------- 节点日志 ----------

function fillDkLogSelect(st) {
  const sel = $('dk-log-node');
  const nodes = (st && st.nodes) || [];
  if (!nodes.length) { sel.innerHTML = '<option value="">（无节点）</option>'; return; }
  const cur = sel.value || String(DK.logNode);
  sel.innerHTML = nodes.map(n => `<option value="${n.id}">node ${n.id}（${escapeHtml(n.name)}）</option>`).join('');
  if ([...sel.options].some(o => o.value === cur)) sel.value = cur;
}

async function dkLoadLogs() {
  const id = $('dk-log-node').value;
  if (!id) return;
  DK.logNode = Number(id);
  const view = $('dk-log-view');
  view.innerHTML = '<span class="muted">加载中…</span>';
  try {
    const resp = await opsApi(`/docker/logs/${id}?lines=300`);
    const text = resp.log || '';
    view.innerHTML = text
      ? text.split('\n').map(l => `<div class="log-line">${escapeHtml(l)}</div>`).join('')
      : '<span class="muted">（空日志）</span>';
  } catch (e) {
    view.innerHTML = `<span class="muted">加载失败：${escapeHtml(e.message)}</span>`;
  }
}

$('dk-log-node').addEventListener('change', dkLoadLogs);
$('dk-log-refresh').addEventListener('click', dkLoadLogs);

// ---------- 运维轮询 ----------
dkConfigLoad();
dkPoll();
setInterval(dkPoll, 5000);
