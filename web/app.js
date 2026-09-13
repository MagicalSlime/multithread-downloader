/* 多线程下载器 —— 界面逻辑
 *
 * 与 Python 的分工：Python 只提供数据和动作（window.pywebview.api），
 * 这里负责全部渲染。刷新沿用「高频轮询 + 低频事件」两条通道：
 *   - snapshot(): 每 100ms 拉一次所有任务的状态（高频数值）
 *   - drain_events(): 每 100ms 取一次积压的离散事件（日志、完成、取消…）
 * 这样 Python 侧永远不用从工作线程往界面推东西，省掉一整类线程安全问题。
 */

'use strict';

const $ = (id) => document.getElementById(id);

const RATE_CHOICES = [
  ['不限速', 0], ['128 KB/s', 128 << 10], ['256 KB/s', 256 << 10],
  ['512 KB/s', 512 << 10], ['1 MB/s', 1 << 20], ['2 MB/s', 2 << 20],
  ['5 MB/s', 5 << 20], ['10 MB/s', 10 << 20],
];

const STATE_TEXT = {
  idle: ['等待开始', 'idle'], queued: ['排队中', 'idle'],
  cancelled: ['已取消', 'idle'], probing: ['连接中', 'run'],
  running: ['下载中', 'run'], paused: ['已暂停', 'warn'],
  done: ['已完成', 'ok'], failed: ['失败', 'err'],
};

// 状态 -> 三个按钮：[文字, 动作]，文字为空表示不显示
const BUTTONS = {
  idle: [['开始', 'start'], ['移除', 'remove'], ['', '']],
  queued: [['取消', 'cancel'], ['', ''], ['', '']],
  probing: [['暂停', 'pause'], ['取消', 'cancel'], ['', '']],
  running: [['暂停', 'pause'], ['取消', 'cancel'], ['', '']],
  paused: [['继续', 'start'], ['取消', 'cancel'], ['', '']],
  cancelled: [['继续', 'start'], ['移除', 'remove'], ['', '']],
  done: [['打开', 'open'], ['打开目录', 'reveal'], ['移除', 'remove']],
  failed: [['重试', 'start'], ['打开目录', 'reveal'], ['移除', 'remove']],
};

let rows = new Map();       // task id -> {el, refs}
let latest = new Map();     // task id -> 最近一次快照，判断状态用
let pending = null;         // 正在弹窗确认的操作 {kind, id}

const api = (name, ...args) => window.pywebview.api[name](...args);

/* ------------------------------------------------------------------ 工具 */

function bytes(n) {
  if (n === null || n === undefined) return '未知';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return (i === 0 ? n.toFixed(0) : n.toFixed(2)) + ' ' + units[i];
}

function speed(n) {
  return (!n || n <= 0) ? '-- B/s' : bytes(n) + '/s';
}

function eta(sec) {
  if (sec === null || sec === undefined || sec < 0) return '未知';
  sec = Math.floor(sec);
  const pad = (x) => String(x).padStart(2, '0');
  return sec < 3600
    ? pad(Math.floor(sec / 60)) + ':' + pad(sec % 60)
    : Math.floor(sec / 3600) + ':' + pad(Math.floor((sec % 3600) / 60)) + ':' + pad(sec % 60);
}

function toast(message) {
  const el = $('toast');
  el.textContent = message;
  el.classList.add('show');
  clearTimeout(el._t);
  el._t = setTimeout(() => el.classList.remove('show'), 1800);
}

/* ------------------------------------------------------------------ 渲染 */

function createRow(task) {
  const el = document.createElement('div');
  el.className = 'task';
  el.innerHTML = `
    <div class="task-head">
      <div class="task-name"></div>
      <div class="task-right">
        <span class="task-pct"></span>
        <span class="chip"></span>
      </div>
    </div>
    <div class="bar"><div class="bar-fill"></div></div>
    <div class="task-foot">
      <div class="task-detail"></div>
      <div class="task-actions"></div>
    </div>`;

  const refs = {
    name: el.querySelector('.task-name'),
    pct: el.querySelector('.task-pct'),
    chip: el.querySelector('.chip'),
    bar: el.querySelector('.bar'),
    fill: el.querySelector('.bar-fill'),
    detail: el.querySelector('.task-detail'),
    actions: el.querySelector('.task-actions'),
    buttons: [],
    spec: null,
    state: null,
    indeterminate: false,
  };

  for (let i = 0; i < 3; i++) {
    const btn = document.createElement('button');
    btn.className = 'btn mini';
    btn.style.display = 'none';
    btn.addEventListener('click', () => {
      const action = btn.dataset.action;
      if (action) doAction(task.id, action);
    });
    refs.actions.appendChild(btn);
    refs.buttons.push(btn);
  }

  return { el, refs };
}

function updateRow(row, task) {
  const r = row.refs;

  if (r.name.textContent !== task.name) r.name.textContent = task.name;
  if (r.detail.textContent !== task.detail) r.detail.textContent = task.detail;
  r.detail.classList.toggle('error', task.state === 'failed');

  const [label, tone] = STATE_TEXT[task.state] || [task.state, 'idle'];
  if (r.state !== task.state) {
    r.chip.textContent = label;
    r.chip.className = 'chip ' + tone;
    r.state = task.state;
  }

  // 进度条：长度未知且正在下载时切成不确定态（来回滑动），其余一律确定态。
  // 必须双向切换——只在「长度已知」时处理的话，「长度未知 + 任务结束」
  // 就会卡在动画里，探测失败就属于这种情况。
  const animate = task.total <= 0 && (task.state === 'running' || task.state === 'probing');
  if (animate !== r.indeterminate) {
    r.bar.classList.toggle('indeterminate', animate);
    r.indeterminate = animate;
  }

  if (task.total > 0) {
    r.fill.style.width = (task.percent || 0) + '%';
    r.pct.textContent = (task.percent || 0).toFixed(1) + '%';
  } else {
    r.fill.style.width = '0%';
    r.pct.textContent = '';
  }

  const spec = BUTTONS[task.state] || BUTTONS.idle;
  if (JSON.stringify(spec) !== JSON.stringify(r.spec)) {
    r.spec = spec;
    spec.forEach(([text, action], i) => {
      const btn = r.buttons[i];
      if (!text) { btn.style.display = 'none'; return; }
      btn.textContent = text;
      btn.dataset.action = action;
      btn.style.display = '';
    });
  }
}

function renderTasks(tasks) {
  const list = $('list');
  const seen = new Set();
  latest = new Map(tasks.map((t) => [t.id, t]));

  for (const task of tasks) {
    seen.add(task.id);
    let row = rows.get(task.id);
    if (!row) {
      row = createRow(task);
      rows.set(task.id, row);
      list.appendChild(row.el);
    }
    updateRow(row, task);
  }

  for (const [id, row] of Array.from(rows)) {
    if (!seen.has(id)) { row.el.remove(); rows.delete(id); }
  }

  $('empty').style.display = tasks.length ? 'none' : '';
  $('section-head').textContent = tasks.length
    ? `下载任务（${tasks.length}）` : '下载任务';
}

function appendLog(entry) {
  const log = $('log');
  const line = document.createElement('div');
  if (entry.tag) line.className = entry.tag;
  const time = document.createElement('span');
  time.className = 'time';
  time.textContent = '[' + entry.time + '] ';
  line.appendChild(time);
  line.appendChild(document.createTextNode(entry.text));
  log.appendChild(line);

  while (log.childElementCount > 2000) log.removeChild(log.firstChild);
  if ($('autoscroll').classList.contains('on')) log.scrollTop = log.scrollHeight;
}

/* ------------------------------------------------------------------ 动作 */

async function doAction(taskId, action) {
  if (action === 'cancel') {
    const info = await api('prepare_cancel', taskId);
    if (info && info.done > 0) {
      // 有东西可删才值得问一句，否则直接取消
      openModal('取消下载',
        `已下载 ${bytes(info.done)}。临时文件是删掉，还是留着方便以后续传？`,
        { kind: 'cancel', id: taskId },
        [['不取消', null], ['保留临时文件', false], ['删除临时文件', true]]);
      return;
    }
    await api('cancel_task', taskId, false);
    return;
  }

  if (action === 'remove') {
    const task = latest.get(taskId);
    if (task && (task.state === 'probing' || task.state === 'running')) {
      openModal('移除任务', '任务正在下载，移除会中止它。确定吗？',
        { kind: 'remove', id: taskId },
        [['不取消', null], null, ['确认移除', true]]);
      return;
    }
    await api('remove_task', taskId);
    return;
  }

  await api({
    start: 'start_task', pause: 'pause_task',
    open: 'open_file', reveal: 'reveal_file',
  }[action], taskId);
}

// 三个按钮按场景配置：中间那个传 null 就隐藏
function openModal(title, text, context, buttons) {
  pending = context;
  $('modal-title').textContent = title;
  $('modal-text').textContent = text;
  [['modal-back', 0], ['modal-mid', 1], ['modal-main', 2]].forEach(([id, i]) => {
    const spec = buttons[i];
    const el = $(id);
    el.style.display = spec ? '' : 'none';
    if (spec) el.textContent = spec[0];
  });
  $('modal').classList.add('open');
}

function closeModal() {
  $('modal').classList.remove('open');
  pending = null;
}

async function resolveModal(index) {
  const context = pending;
  const specs = {
    cancel: [null, false, true],
    remove: [null, null, true],
  };
  closeModal();
  if (!context) return;
  const choice = specs[context.kind][index];
  if (choice === null || choice === undefined) return;   // 「不取消」
  if (context.kind === 'cancel') await api('cancel_task', context.id, choice);
  else await api('remove_task', context.id, choice);
}

/* ------------------------------------------------------------------ 表单 */

function collectOptions() {
  return {
    threads: parseInt($('threads').value, 10) || 8,
    retries: parseInt($('retries').value, 10) || 0,
    resume: $('resume').classList.contains('on'),
    verify_tls: !$('tls').classList.contains('on'),
    user_agent: $('ua').value.trim(),
    referer: $('referer').value.trim(),
    cookie: $('cookie').value.trim(),
  };
}

async function submit() {
  const url = $('url').value.trim();
  const result = await api('add_task', url, $('save').value.trim(), collectOptions());
  if (!result.ok) { toast(result.error); return; }
  $('url').value = '';
}

function applySettings(s) {
  $('save').value = s.save_dir || '';
  $('threads').value = s.threads;
  $('retries').value = s.max_retries;
  $('ua').value = s.user_agent || '';
  $('referer').value = s.referer || '';
  $('concurrent').value = s.max_concurrent;

  $('resume').classList.toggle('on', !!s.resume);
  $('tls').classList.toggle('on', !s.verify_tls);
  $('autoscroll').classList.toggle('on', !!s.autoscroll_log);

  const sel = $('rate');
  RATE_CHOICES.forEach(([label, value], i) => {
    const opt = document.createElement('option');
    opt.value = String(value);
    opt.textContent = label;
    sel.appendChild(opt);
    if (value === s.rate_limit) sel.selectedIndex = i;
  });
}

/* ------------------------------------------------------------------ 轮询 */

let pumpTimer = null;
let shuttingDown = false;

async function pump() {
  if (shuttingDown) return;
  try {
    const tasks = await api('snapshot');
    renderTasks(tasks);

    const events = await api('drain_events');
    if (events.length) events.forEach(appendLog);
  } catch (err) {
    /* 窗口正在关闭时调用会失败，忽略即可 */
  }
}

// 窗口一进入关闭流程就停止轮询。
// 实测：关闭的瞬间还有 Python 调用在途时，WebView2 那边有概率挂住不响应
// （同一份代码，带轮询时 8 次里只有 2 次能正常关，停掉轮询后 5 次）。
window.addEventListener('beforeunload', () => {
  shuttingDown = true;
  if (pumpTimer !== null) clearInterval(pumpTimer);
  pumpTimer = null;
});

/* ------------------------------------------------------------------ 绑定 */

function bind() {
  $('add').addEventListener('click', submit);
  $('url').addEventListener('keydown', (e) => { if (e.key === 'Enter') submit(); });

  $('paste').addEventListener('click', async () => {
    const text = await api('clipboard');
    if (!text) { toast('剪贴板里没有文本'); return; }
    const match = text.match(/https?:\/\/[^\s"'<>()]+/i);
    $('url').value = match ? match[0] : text.split(/\s+/)[0];
    $('url').focus();
  });

  $('browse').addEventListener('click', async () => {
    const path = await api('browse', $('url').value.trim(), $('save').value.trim());
    if (path) $('save').value = path;
  });

  $('advanced-toggle').addEventListener('click', () => {
    const open = $('advanced').classList.toggle('open');
    $('advanced-toggle').textContent = (open ? '▾' : '▸') + ' 高级设置';
  });

  ['resume', 'tls', 'autoscroll'].forEach((id) => {
    $(id).addEventListener('click', () => $(id).classList.toggle('on'));
  });

  $('start-all').addEventListener('click', () => api('start_all'));
  $('pause-all').addEventListener('click', () => api('pause_all'));
  $('clear-done').addEventListener('click', () => api('clear_finished'));
  $('clear-log').addEventListener('click', () => { $('log').textContent = ''; });

  $('concurrent').addEventListener('change', () => {
    api('set_concurrency', parseInt($('concurrent').value, 10) || 3);
  });
  $('rate').addEventListener('change', () => {
    api('set_rate_limit', parseInt($('rate').value, 10) || 0);
  });

  $('modal').addEventListener('click', (e) => {
    if (e.target === $('modal')) { closeModal(); return; }
    const index = { 'modal-back': 0, 'modal-mid': 1, 'modal-main': 2 }[e.target.id];
    if (index !== undefined) resolveModal(index);
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && $('modal').classList.contains('open')) closeModal();
  });
}

async function boot() {
  bind();
  applySettings(await api('get_settings'));
  pump();
  pumpTimer = setInterval(pump, 100);
}

if (window.pywebview && window.pywebview.api) boot();
else window.addEventListener('pywebviewready', boot);
