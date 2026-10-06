// Verify service entry point. Runs once and exits:
//   1. rule tests (validation, idempotency, conflicts, hash chain)
//   2. interrupt-recovery drills (crash after each persistence stage)
//   3. concurrency acceptance (two connections interleaving submissions)
//   4. frontend build check
//   5. page + health HTTP smoke against the web service
// Exit code 0 = all checks passed, 1 = at least one failure.

import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, stat } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { registerRuleTests } from "./tests/rules.test.js";
import { registerRecoveryTests } from "./tests/recovery.test.js";
import { registerConcurrencyTests } from "./tests/concurrency.test.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const WEB_URL = (process.env.WEB_URL ?? "http://web:8080").replace(/\/$/, "");

// ---- tiny test harness -----------------------------------------------------

const tests = [];
const harness = {
  test(name, fn) {
    tests.push({ name, fn });
  },
  assert(condition, message) {
    if (!condition) throw new Error(`assertion failed: ${message}`);
  },
  assertEqual(actual, expected, message = "") {
    if (actual !== expected) {
      throw new Error(
        `expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}${message ? ` — ${message}` : ""}`
      );
    }
  },
  assertDeepEqual(actual, expected, message = "") {
    const a = JSON.stringify(actual);
    const b = JSON.stringify(expected);
    if (a !== b) {
      throw new Error(`deep equal failed: expected ${b}, got ${a}${message ? ` — ${message}` : ""}`);
    }
  },
};

registerRuleTests(harness);
registerRecoveryTests(harness);
registerConcurrencyTests(harness);

const results = [];

async function runUnitTests() {
  for (const { name, fn } of tests) {
    try {
      await fn();
      results.push({ name, ok: true });
    } catch (err) {
      results.push({ name, ok: false, error: err?.stack ?? String(err) });
    }
  }
}

// ---- frontend build check --------------------------------------------------

async function checkFrontendBuild() {
  const name = "build: frontend bundles cleanly";
  const outDir = await mkdtemp(path.join(os.tmpdir(), "glider-build-"));
  const run = spawnSync("node", ["frontend/build.js", "--out", outDir], {
    cwd: repoRoot,
    encoding: "utf8",
  });
  try {
    if (run.status !== 0) {
      throw new Error(`build exited ${run.status}: ${run.stderr || run.stdout}`);
    }
    const appJs = path.join(outDir, "app.js");
    const [appStat, indexHtml, styles] = await Promise.all([
      stat(appJs),
      readFile(path.join(outDir, "index.html"), "utf8"),
      stat(path.join(outDir, "styles.css")),
    ]);
    if (appStat.size < 10_000) {
      throw new Error(`app.js suspiciously small (${appStat.size} bytes)`);
    }
    if (styles.size === 0) throw new Error("styles.css is empty");
    if (!indexHtml.includes('id="app"') || !indexHtml.includes("app.js")) {
      throw new Error("index.html is missing the app mount or script tag");
    }
    const bundle = await readFile(appJs, "utf8");
    for (const marker of ["afterPrepare", "afterSegment", "afterManifest", "indexedDB"]) {
      if (!bundle.includes(marker)) {
        throw new Error(`bundle is missing expected marker ${JSON.stringify(marker)}`);
      }
    }
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: err?.stack ?? String(err) });
  }
}

// ---- HTTP smoke ------------------------------------------------------------

async function fetchWithRetry(url, attempts = 15, delayMs = 500) {
  let lastError;
  for (let i = 0; i < attempts; i += 1) {
    try {
      return await fetch(url, { signal: AbortSignal.timeout(4000) });
    } catch (err) {
      lastError = err;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  throw lastError;
}

async function smoke(name, url, check) {
  try {
    const res = await fetchWithRetry(url);
    const body = await res.text();
    await check(res, body);
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: err?.stack ?? String(err) });
  }
}

async function runHttpSmoke() {
  await smoke("http: GET /health responds ok", `${WEB_URL}/health`, async (res, body) => {
    if (res.status !== 200) throw new Error(`status ${res.status}`);
    const json = JSON.parse(body);
    if (json.status !== "ok") throw new Error(`unexpected body ${body}`);
  });

  await smoke("http: GET / serves the review page", `${WEB_URL}/`, async (res, body) => {
    if (res.status !== 200) throw new Error(`status ${res.status}`);
    if (!(res.headers.get("content-type") ?? "").includes("text/html")) {
      throw new Error(`unexpected content-type ${res.headers.get("content-type")}`);
    }
    if (!body.includes('id="app"') || !body.includes("app.js")) {
      throw new Error("page HTML missing app mount or bundle script");
    }
  });

  await smoke("http: GET /app.js serves the bundle", `${WEB_URL}/app.js`, async (res, body) => {
    if (res.status !== 200) throw new Error(`status ${res.status}`);
    if (body.length < 10_000) throw new Error(`bundle too small (${body.length})`);
  });
}

// ---- main ------------------------------------------------------------------

console.log("== glider-seal verify ==");
console.log(`web target: ${WEB_URL}\n`);

await runUnitTests();
await checkFrontendBuild();
await runHttpSmoke();

let failed = 0;
for (const result of results) {
  if (result.ok) {
    console.log(`PASS  ${result.name}`);
  } else {
    failed += 1;
    console.log(`FAIL  ${result.name}`);
    console.log(
      String(result.error)
        .split("\n")
        .map((line) => `      ${line}`)
        .join("\n")
    );
  }
}

console.log(`\n${results.length - failed}/${results.length} checks passed`);
if (failed > 0) {
  console.log("verify: FAILED");
  process.exit(1);
}
console.log("verify: OK");
process.exit(0);
