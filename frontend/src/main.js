import { SealEngine, CrashError, CRASH_STAGES } from "../../core/engine.js";
import { IdbStorage } from "./idb-store.js";

const $ = (id) => document.getElementById(id);

const short = (hex) => (hex ? `${hex.slice(0, 12)}…` : "—");

let storage = null;
let engine = null;
let lastRecovery = null;

// ---- rendering -----------------------------------------------------------

function renderChain(chain) {
  const tbody = $("chain-body");
  tbody.textContent = "";
  const actionByDigest = new Map();
  for (const action of lastRecovery?.actions ?? []) {
    if (action.digest) actionByDigest.set(action.digest, action.action);
  }
  if (chain.segments.length === 0) {
    const row = tbody.insertRow();
    const cell = row.insertCell();
    cell.colSpan = 6;
    cell.className = "empty";
    cell.textContent = "封存链为空 — 尚无已封存记录";
  }
  for (const segment of chain.segments) {
    const row = tbody.insertRow();
    row.insertCell().textContent = segment.batchId;
    row.insertCell().textContent = `${segment.seqStart} – ${segment.seqEnd} (${segment.events.length} 条)`;
    const prev = row.insertCell();
    prev.className = "mono";
    prev.textContent = short(segment.prevDigest);
    prev.title = segment.prevDigest;
    const digest = row.insertCell();
    digest.className = "mono";
    digest.textContent = short(segment.digest);
    digest.title = segment.digest;
    row.insertCell().textContent =
      actionByDigest.get(segment.digest) ?? "—";
    const receipt = row.insertCell();
    receipt.className = "mono";
    receipt.textContent = short(segment.receipt.digest);
    receipt.title = JSON.stringify(segment.receipt);
  }
  $("chain-head").textContent = short(chain.head);
  $("chain-version").textContent = String(chain.version);
  $("chain-count").textContent = String(chain.segments.length);
}

function renderRecovery() {
  const list = $("recovery-actions");
  list.textContent = "";
  const actions = lastRecovery?.actions ?? [];
  if (actions.length === 0) {
    const li = document.createElement("li");
    li.className = "empty";
    li.textContent = "本次打开无恢复动作（残留状态干净）";
    list.appendChild(li);
  }
  for (const action of actions) {
    const li = document.createElement("li");
    const parts = [action.action];
    if (action.batchId) parts.push(`批次 ${action.batchId}`);
    if (action.digest) parts.push(`段 ${short(action.digest)}`);
    if (action.reason) parts.push(`原因 ${action.reason}`);
    if (action.droppedDigests) {
      parts.push(`丢弃 ${action.droppedDigests.length} 段`);
    }
    li.textContent = parts.join(" · ");
    list.appendChild(li);
  }
  $("recovery-at").textContent = lastRecovery?.at ?? "—";

  const blockingBox = $("first-blocking");
  blockingBox.textContent = "";
  if (lastRecovery?.firstBlocking) {
    blockingBox.classList.add("blocking");
    blockingBox.textContent = JSON.stringify(lastRecovery.firstBlocking, null, 2);
  } else {
    blockingBox.classList.remove("blocking");
    blockingBox.textContent = "无阻断证据";
  }
}

function renderConflicts(conflicts) {
  const list = $("conflict-list");
  list.textContent = "";
  if (conflicts.length === 0) {
    const li = document.createElement("li");
    li.className = "empty";
    li.textContent = "无冲突记录";
    list.appendChild(li);
    return;
  }
  for (const conflict of conflicts) {
    const li = document.createElement("li");
    li.textContent = `${conflict.at} · 批次 ${conflict.batchId} · ${conflict.reason} · 既有内容 ${short(conflict.existingContentHash)} / 传入内容 ${short(conflict.incomingContentHash)}`;
    list.appendChild(li);
  }
}

function renderOutcome(outcome) {
  const box = $("submit-result");
  box.textContent = JSON.stringify(outcome, null, 2);
  box.dataset.status = outcome.status ?? "";
}

async function refresh() {
  const chain = await engine.sealedChain();
  lastRecovery = await engine.lastRecovery();
  renderChain(chain);
  renderRecovery();
  renderConflicts(await engine.conflicts());
}

// ---- batch form ------------------------------------------------------------

function parseEvents(text) {
  const events = [];
  const lines = text.split("\n");
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (line === "") continue;
    const sep = line.indexOf("|");
    if (sep === -1) {
      throw new Error(`第 ${i + 1} 行缺少「|」分隔符（格式：序号|载荷）`);
    }
    const seq = Number(line.slice(0, sep));
    if (!Number.isSafeInteger(seq)) {
      throw new Error(`第 ${i + 1} 行序号不是整数`);
    }
    events.push({ seq, payload: line.slice(sep + 1) });
  }
  return events;
}

async function onSubmit(event) {
  event.preventDefault();
  let batch;
  try {
    batch = {
      batchId: $("batch-id").value.trim(),
      events: parseEvents($("events-input").value),
    };
  } catch (err) {
    renderOutcome({ status: "rejected", error: { code: "parse-error", detail: err.message } });
    return;
  }
  const outcome = await engine.submit(batch);
  renderOutcome(outcome);
  await refresh();
}

function onFillDemo() {
  $("batch-id").value = `BATCH-${new Date().toISOString().slice(0, 10)}-01`;
  $("events-input").value = [
    "101|姿态机动开始，滚转角 +12.5°",
    "102|太阳帆板展开确认",
    "103|星敏感器锁定，姿态解算正常",
  ].join("\n");
}

// ---- fault drills ------------------------------------------------------------

async function onRunDrill() {
  const stage = $("drill-stage").value;
  const log = $("drill-log");
  const chain = await engine.sealedChain();
  const lastSeq = chain.segments.length
    ? chain.segments[chain.segments.length - 1].seqEnd
    : 0;
  const batchId = `DRILL-${Date.now().toString(36).toUpperCase()}`;
  const batch = {
    batchId,
    events: [1, 2, 3].map((n) => ({
      seq: lastSeq + n,
      payload: `演练事件 ${n}（中断点 ${stage}）`,
    })),
  };

  log.textContent = `演练批次 ${batchId}，在 ${stage} 后注入中断…\n`;

  // A drill engine shares the live store but crashes after the chosen stage.
  const drillEngine = new SealEngine(storage, { failpoints: { [stage]: true } });
  let crash = null;
  try {
    await drillEngine.submit(batch);
  } catch (err) {
    if (err instanceof CrashError) crash = err;
    else throw err;
  }
  if (!crash) {
    log.textContent += "未发生中断（异常）\n";
    return;
  }
  log.textContent += `已中断：${crash.message}\n正在模拟下一次打开（关闭并重开 IndexedDB 连接）…\n`;

  // Simulate the next open: drop the connection, reopen, run recovery.
  storage.close();
  storage = await IdbStorage.open();
  engine = new SealEngine(storage);
  const { recovery } = await engine.open();
  lastRecovery = recovery;

  log.textContent += `恢复完成：${recovery.actions.length} 个动作，阻断证据 ${recovery.blocking.length} 条\n`;
  for (const action of recovery.actions) {
    log.textContent += `  · ${action.action}${action.batchId ? ` · ${action.batchId}` : ""}\n`;
  }
  if (recovery.firstBlocking) {
    log.textContent += `首个阻断证据：${JSON.stringify(recovery.firstBlocking)}\n`;
  }
  await refresh();
}

// ---- boot ---------------------------------------------------------------

async function boot() {
  storage = await IdbStorage.open();
  engine = new SealEngine(storage);
  const { recovery } = await engine.open();
  lastRecovery = recovery;

  for (const stage of CRASH_STAGES) {
    const option = document.createElement("option");
    option.value = stage;
    option.textContent = {
      afterPrepare: "准备意图写入后（afterPrepare）",
      afterSegment: "段写入后（afterSegment）",
      afterManifest: "清单切换后（afterManifest）",
    }[stage];
    $("drill-stage").appendChild(option);
  }

  $("submit-form").addEventListener("submit", (e) => {
    onSubmit(e).catch((err) => renderOutcome({ status: "error", error: String(err) }));
  });
  $("fill-demo").addEventListener("click", onFillDemo);
  $("run-drill").addEventListener("click", () => {
    onRunDrill().catch((err) => {
      $("drill-log").textContent += `演练失败：${err}\n`;
    });
  });
  $("reload-page").addEventListener("click", () => location.reload());

  await refresh();
}

boot().catch((err) => {
  document.body.insertAdjacentHTML(
    "afterbegin",
    `<pre class="fatal">初始化失败：${err.stack ?? err}</pre>`
  );
});
