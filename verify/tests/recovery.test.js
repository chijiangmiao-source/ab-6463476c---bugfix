// Interrupt-recovery drills: crash after each persistence stage, reopen,
// and verify the recovery contract — incomplete or corrupt prepared
// segments never enter the chain, complete unpublished intents recover to
// a unique outcome, and published segments are never appended twice.

import { SealEngine, CrashError } from "../../core/engine.js";
import {
  MemoryStorage,
  createMemoryBacking,
} from "../../core/memory-storage.js";
import { GENESIS_DIGEST, segmentDigestOf } from "../../core/canonical.js";

function batch(batchId, seqs, payloadPrefix = "event") {
  return {
    batchId,
    events: seqs.map((seq, i) => ({ seq, payload: `${payloadPrefix}-${i}` })),
  };
}

/** Open an engine over the backing, submit, and expect a crash at `stage`. */
async function crashAt(backing, stage, submitBatch) {
  const engine = new SealEngine(new MemoryStorage(backing), {
    failpoints: { [stage]: true },
  });
  await engine.open();
  try {
    await engine.submit(submitBatch);
  } catch (err) {
    if (err instanceof CrashError) {
      return err;
    }
    throw err;
  }
  throw new Error(`expected a CrashError after ${stage}`);
}

/** Reopen a fresh engine over the same persisted backing. */
async function reopen(backing) {
  const engine = new SealEngine(new MemoryStorage(backing));
  const { recovery, chain } = await engine.open();
  return { engine, recovery, chain };
}

export function registerRecoveryTests(t) {
  t.test("recovery: crash after prepare intent -> intent discarded, chain empty", async () => {
    const backing = createMemoryBacking();
    const crash = await crashAt(backing, "afterPrepare", batch("P-1", [1, 2, 3]));
    t.assertEqual(crash.stage, "afterPrepare");
    t.assertEqual(backing.prepares.size, 1);
    t.assertEqual(backing.segments.size, 0);

    const { recovery, chain } = await reopen(backing);
    t.assertEqual(chain.segments.length, 0);
    t.assertEqual(recovery.blocking.length, 0);
    t.assert(
      recovery.actions.some((a) => a.action === "intent-discarded-incomplete" && a.batchId === "P-1"),
      "expected intent-discarded-incomplete action"
    );
    t.assertEqual(backing.prepares.size, 0, "leftover intent swept");
  });

  t.test("recovery: crash after segment write -> unique publish per intent", async () => {
    const backing = createMemoryBacking();
    const submitted = batch("S-1", [10, 11], "sealed");
    await crashAt(backing, "afterSegment", submitted);
    t.assertEqual(backing.segments.size, 1);
    t.assertEqual(backing.manifest, null, "manifest not yet switched");

    const expectedDigest = await segmentDigestOf(GENESIS_DIGEST, submitted.events);
    const { engine, recovery, chain } = await reopen(backing);
    t.assertEqual(chain.segments.length, 1);
    t.assertEqual(chain.segments[0].digest, expectedDigest);
    t.assertEqual(chain.segments[0].prevDigest, GENESIS_DIGEST);
    t.assert(
      recovery.actions.some(
        (a) => a.action === "segment-published-from-intent" && a.batchId === "S-1"
      ),
      "expected segment-published-from-intent action"
    );

    // Retransmission after recovery: original receipt, no second append.
    const again = await engine.submit(submitted);
    t.assertEqual(again.status, "duplicate");
    t.assertEqual(again.receipt.digest, expectedDigest);
    const chainAfter = await engine.sealedChain();
    t.assertEqual(chainAfter.segments.length, 1);
    t.assertEqual(chainAfter.version, 1, "manifest switched exactly once");
  });

  t.test("recovery: crash after manifest switch -> no duplicate append", async () => {
    const backing = createMemoryBacking();
    const submitted = batch("M-1", [1], "manifest");
    await crashAt(backing, "afterManifest", submitted);
    t.assertEqual(backing.manifest.segmentIds.length, 1, "manifest switched before crash");
    t.assertEqual(backing.prepares.size, 1, "fulfilled intent not yet swept");

    const { engine, recovery, chain } = await reopen(backing);
    t.assertEqual(chain.segments.length, 1, "no duplicate append");
    t.assert(
      recovery.actions.some((a) => a.action === "fulfilled-intent-cleaned" && a.batchId === "M-1"),
      "expected fulfilled-intent-cleaned action"
    );
    t.assertEqual(backing.prepares.size, 0);

    const again = await engine.submit(submitted);
    t.assertEqual(again.status, "duplicate");
    t.assertEqual((await engine.sealedChain()).segments.length, 1);
  });

  t.test("recovery: corrupt prepared segment never enters the chain", async () => {
    const backing = createMemoryBacking();
    const submitted = batch("C-1", [4, 5], "corruptible");
    await crashAt(backing, "afterSegment", submitted);

    // Tamper with the persisted segment bytes (half-written / corrupted).
    const [digest, segment] = [...backing.segments.entries()][0];
    segment.events[0].payload = "corrupted-payload";
    backing.segments.set(digest, segment);

    const { engine, recovery, chain } = await reopen(backing);
    t.assertEqual(chain.segments.length, 0, "corrupt segment excluded");
    t.assert(recovery.firstBlocking, "expected blocking evidence");
    t.assertEqual(recovery.firstBlocking.code, "segment-digest-mismatch");
    t.assertEqual(recovery.firstBlocking.batchId, "C-1");
    t.assert(
      recovery.actions.some((a) => a.action === "prepared-segment-rejected"),
      "expected prepared-segment-rejected action"
    );

    // The batch can be resubmitted cleanly. The store is content-addressed,
    // so the authentic segment rewrites the same key the corrupt bytes sat
    // at; the incident evidence persists in the recovery report.
    const retry = await engine.submit(submitted);
    t.assertEqual(retry.status, "sealed");
    t.assertEqual((await engine.sealedChain()).segments.length, 1);
    t.assertEqual(backing.segments.size, 1, "corrupt key self-healed by authentic content");
    const healed = backing.segments.get(retry.receipt.digest);
    t.assertEqual(healed.events[0].payload, "corruptible-0");
    t.assertEqual(
      (await engine.lastRecovery()).firstBlocking.code,
      "segment-digest-mismatch",
      "blocking evidence retained in the persisted recovery report"
    );
  });

  t.test("recovery: stale intent predecessor is blocked, not chained", async () => {
    const backing = createMemoryBacking();
    // Seal one batch normally so the head moves away from genesis.
    const first = await new SealEngine(new MemoryStorage(backing));
    await first.open();
    await first.submit(batch("HEAD-1", [1, 2]));

    // Hand-craft a leftover intent + segment bound to the old (genesis) head.
    const staleEvents = [{ seq: 3, payload: "stale" }];
    const staleDigest = await segmentDigestOf(GENESIS_DIGEST, staleEvents);
    const storage = new MemoryStorage(backing);
    await storage.putPrepare({
      batchId: "STALE-1",
      contentHash: "0".repeat(64),
      events: staleEvents,
      prevDigest: GENESIS_DIGEST,
      expectedDigest: staleDigest,
      createdAt: Date.now(),
    });
    await storage.putSegment({
      digest: staleDigest,
      batchId: "STALE-1",
      contentHash: "0".repeat(64),
      prevDigest: GENESIS_DIGEST,
      seqStart: 3,
      seqEnd: 3,
      events: staleEvents,
      receipt: {},
    });

    const { recovery, chain } = await reopen(backing);
    t.assertEqual(chain.segments.length, 1, "only the genuinely sealed segment");
    t.assert(recovery.firstBlocking, "expected blocking evidence");
    t.assertEqual(recovery.firstBlocking.code, "intent-predecessor-stale");
  });

  t.test("recovery: corrupt published segment truncates the chain", async () => {
    const backing = createMemoryBacking();
    const engine = new SealEngine(new MemoryStorage(backing));
    await engine.open();
    const a = await engine.submit(batch("T-1", [1, 2], "a"));
    await engine.submit(batch("T-2", [3, 4], "b"));

    // Corrupt the first published segment on disk.
    const stored = backing.segments.get(a.receipt.digest);
    stored.events[0].payload = "tampered-after-publish";
    backing.segments.set(a.receipt.digest, stored);

    const { recovery, chain } = await reopen(backing);
    t.assertEqual(chain.segments.length, 0, "invalid prefix and everything after it excluded");
    t.assertEqual(recovery.firstBlocking.code, "segment-digest-mismatch");
    t.assert(
      recovery.actions.some(
        (a) => a.action === "chain-truncated" && a.droppedDigests.length === 2
      ),
      "expected chain-truncated action dropping both segments"
    );
    t.assertEqual(backing.segments.size, 2, "dropped segments kept as orphan evidence");
  });

  t.test("recovery: recovery itself is idempotent", async () => {
    const backing = createMemoryBacking();
    await crashAt(backing, "afterSegment", batch("I-1", [1, 2, 3]));
    const first = await reopen(backing);
    t.assertEqual(first.chain.segments.length, 1);

    const second = await reopen(backing);
    t.assertEqual(second.chain.segments.length, 1);
    t.assertEqual(second.recovery.actions.length, 0, "nothing left to repair");
    t.assertEqual(second.recovery.blocking.length, 0);
    t.assertEqual(second.chain.head, first.chain.head);
  });

  t.test("recovery: multi-batch chain stays continuous across a mid-crash", async () => {
    const backing = createMemoryBacking();
    const setup = new SealEngine(new MemoryStorage(backing));
    await setup.open();
    const a = await setup.submit(batch("N-1", [1, 2, 3], "a"));

    await crashAt(backing, "afterSegment", batch("N-2", [4, 5], "b"));

    const { engine, chain } = await reopen(backing);
    t.assertEqual(chain.segments.length, 2);
    t.assertEqual(chain.segments[1].prevDigest, a.receipt.digest);
    t.assertEqual(chain.segments[0].seqEnd + 1, chain.segments[1].seqStart - 1 + 1);
    t.assert(chain.segments[1].seqStart > chain.segments[0].seqEnd, "monotone seqs");

    const c = await engine.submit(batch("N-3", [6], "c"));
    t.assertEqual(c.status, "sealed");
    t.assertEqual(c.receipt.prevDigest, chain.segments[1].digest);
  });

  t.test("recovery: two pending intents recover deterministically in order", async () => {
    const backing = createMemoryBacking();
    // One engine, crash after every segment write: both batches leave a
    // prepared intent + complete segment, manifest never switches.
    const engine = new SealEngine(new MemoryStorage(backing), {
      failpoints: { afterSegment: true },
    });
    await engine.open();
    for (const b of [batch("D-1", [1, 2], "first"), batch("D-2", [3, 4], "second")]) {
      try {
        await engine.submit(b);
        throw new Error("expected crash");
      } catch (err) {
        t.assert(err instanceof CrashError, "expected CrashError");
      }
    }
    t.assertEqual(backing.prepares.size, 2);
    t.assertEqual(backing.segments.size, 2);

    // D-2's intent was recorded against the genesis head. Recovery publishes
    // D-1 first (intent order); D-2's prepared segment no longer fits the
    // head and is rejected — the only outcome consistent with its intent.
    const { engine: reopened, recovery, chain } = await reopen(backing);
    t.assertEqual(chain.segments.length, 1);
    t.assertEqual(chain.segments[0].batchId, "D-1");
    t.assertEqual(recovery.firstBlocking.code, "intent-predecessor-stale");
    t.assertEqual(recovery.firstBlocking.batchId, "D-2");

    // Resubmitted after recovery, D-2 binds to the new head and seals.
    const retry = await reopened.submit(batch("D-2", [3, 4], "second"));
    t.assertEqual(retry.status, "sealed");
    t.assertEqual(retry.receipt.prevDigest, chain.segments[0].digest);
    t.assertEqual((await reopened.sealedChain()).segments.length, 2);
  });

  t.test("recovery: conflict after recovery keeps evidence and reports", async () => {
    const backing = createMemoryBacking();
    await crashAt(backing, "afterSegment", batch("X-1", [1], "original"));
    const { engine } = await reopen(backing);

    const conflict = await engine.submit({
      batchId: "X-1",
      events: [{ seq: 1, payload: "different-content" }],
    });
    t.assertEqual(conflict.status, "conflict");
    t.assertEqual(conflict.conflict.reason, "published-content-mismatch");

    const chain = await engine.sealedChain();
    t.assertEqual(chain.segments.length, 1);
    t.assertEqual(chain.segments[0].events[0].payload, "original-0");
    t.assertEqual((await engine.conflicts()).length, 1);
  });

  t.test("recovery: retry within one session fulfills the recorded intent", async () => {
    const backing = createMemoryBacking();
    const submitted = batch("R-1", [2, 4, 6], "retry");
    const engine = new SealEngine(new MemoryStorage(backing), {
      failpoints: { afterPrepare: true },
    });
    await engine.open();
    let crashed = null;
    try {
      await engine.submit(submitted);
    } catch (err) {
      crashed = err;
    }
    t.assert(crashed instanceof CrashError, "expected crash after prepare");

    // Retry without reopening: the same intent is fulfilled, not duplicated.
    const noCrash = new SealEngine(new MemoryStorage(backing));
    await noCrash.open();
    const retry = await noCrash.submit(submitted);
    t.assertEqual(retry.status, "sealed");
    const expectedDigest = await segmentDigestOf(GENESIS_DIGEST, submitted.events);
    t.assertEqual(retry.receipt.digest, expectedDigest);
    t.assertEqual((await noCrash.sealedChain()).segments.length, 1);
  });
}
