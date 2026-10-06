// Rule tests: batch validation, idempotent retransmission, conflict
// handling, hash-chain binding and sequencing rules.

import { createHash } from "node:crypto";
import { SealEngine } from "../../core/engine.js";
import {
  MemoryStorage,
  createMemoryBacking,
} from "../../core/memory-storage.js";
import { GENESIS_DIGEST } from "../../core/canonical.js";

function sha256Node(text) {
  return createHash("sha256").update(text, "utf8").digest("hex");
}

function canonicalSegment(prevDigest, events) {
  return [prevDigest, ...events.map((e) => `${e.seq}|${e.payload}`)].join("\n");
}

async function freshEngine() {
  const engine = new SealEngine(new MemoryStorage(createMemoryBacking()));
  await engine.open();
  return engine;
}

export function registerRuleTests(t) {
  t.test("rules: rejects malformed batch ids", async () => {
    const engine = await freshEngine();
    for (const batchId of ["", "has space", "x".repeat(65), "-lead", "批次-1"]) {
      const outcome = await engine.submit({
        batchId,
        events: [{ seq: 1, payload: "a" }],
      });
      t.assertEqual(outcome.status, "rejected", `batchId ${JSON.stringify(batchId)}`);
      t.assertEqual(outcome.error.code, "invalid-batch-id");
    }
  });

  t.test("rules: rejects empty and oversized event lists", async () => {
    const engine = await freshEngine();
    const empty = await engine.submit({ batchId: "B1", events: [] });
    t.assertEqual(empty.error.code, "events-empty");

    const tooMany = await engine.submit({
      batchId: "B2",
      events: Array.from({ length: 25 }, (_, i) => ({ seq: i + 1, payload: "x" })),
    });
    t.assertEqual(tooMany.error.code, "events-too-many");
  });

  t.test("rules: accepts exactly 24 events (upper bound)", async () => {
    const engine = await freshEngine();
    const outcome = await engine.submit({
      batchId: "B24",
      events: Array.from({ length: 24 }, (_, i) => ({ seq: i + 1, payload: `e${i}` })),
    });
    t.assertEqual(outcome.status, "sealed");
    t.assertEqual(outcome.receipt.count, 24);
  });

  t.test("rules: requires strictly increasing integer seqs", async () => {
    const engine = await freshEngine();
    const equal = await engine.submit({
      batchId: "B3",
      events: [
        { seq: 2, payload: "a" },
        { seq: 2, payload: "b" },
      ],
    });
    t.assertEqual(equal.error.code, "seq-not-increasing");

    const decreasing = await engine.submit({
      batchId: "B4",
      events: [
        { seq: 5, payload: "a" },
        { seq: 3, payload: "b" },
      ],
    });
    t.assertEqual(decreasing.error.code, "seq-not-increasing");

    const fractional = await engine.submit({
      batchId: "B5",
      events: [{ seq: 1.5, payload: "a" }],
    });
    t.assertEqual(fractional.error.code, "invalid-seq");

    const nonString = await engine.submit({
      batchId: "B6",
      events: [{ seq: 1, payload: 42 }],
    });
    t.assertEqual(nonString.error.code, "invalid-payload");
  });

  t.test("rules: digest matches independent SHA-256 of canonical UTF-8", async () => {
    const engine = await freshEngine();
    const events = [
      { seq: 7, payload: "姿态机动 Δv=0.3" },
      { seq: 9, payload: "emoji 🚀 and combining é chars" },
    ];
    const outcome = await engine.submit({ batchId: "HASH-1", events });
    t.assertEqual(outcome.status, "sealed");
    const expected = sha256Node(canonicalSegment(GENESIS_DIGEST, events));
    t.assertEqual(outcome.receipt.digest, expected);
    t.assertEqual(outcome.receipt.prevDigest, GENESIS_DIGEST);
  });

  t.test("rules: new segment binds to the previous segment digest", async () => {
    const engine = await freshEngine();
    const first = await engine.submit({
      batchId: "LINK-1",
      events: [{ seq: 1, payload: "one" }],
    });
    const second = await engine.submit({
      batchId: "LINK-2",
      events: [{ seq: 2, payload: "two" }],
    });
    t.assertEqual(second.receipt.prevDigest, first.receipt.digest);
    const expected = sha256Node(
      canonicalSegment(first.receipt.digest, [{ seq: 2, payload: "two" }])
    );
    t.assertEqual(second.receipt.digest, expected);
  });

  t.test("rules: identical retransmission returns the original receipt", async () => {
    const engine = await freshEngine();
    const batch = {
      batchId: "DUP-1",
      events: [
        { seq: 1, payload: "alpha" },
        { seq: 2, payload: "beta" },
      ],
    };
    const first = await engine.submit(batch);
    t.assertEqual(first.status, "sealed");
    const chainBefore = await engine.sealedChain();

    const again = await engine.submit(batch);
    t.assertEqual(again.status, "duplicate");
    t.assertDeepEqual(again.receipt, first.receipt);

    const chainAfter = await engine.sealedChain();
    t.assertEqual(chainAfter.segments.length, 1);
    t.assertEqual(chainAfter.version, chainBefore.version);
  });

  t.test("rules: same id with different content conflicts, evidence kept", async () => {
    const engine = await freshEngine();
    const original = await engine.submit({
      batchId: "CONF-1",
      events: [{ seq: 1, payload: "original" }],
    });
    const conflict = await engine.submit({
      batchId: "CONF-1",
      events: [{ seq: 1, payload: "tampered" }],
    });
    t.assertEqual(conflict.status, "conflict");
    t.assertEqual(conflict.conflict.reason, "published-content-mismatch");
    t.assertDeepEqual(conflict.conflict.existingReceipt, original.receipt);

    const chain = await engine.sealedChain();
    t.assertEqual(chain.segments.length, 1);
    t.assertEqual(chain.segments[0].events[0].payload, "original");

    const conflicts = await engine.conflicts();
    t.assertEqual(conflicts.length, 1);
    t.assertEqual(conflicts[0].batchId, "CONF-1");
  });

  t.test("rules: sequence regression across batches is rejected", async () => {
    const engine = await freshEngine();
    await engine.submit({
      batchId: "SEQ-1",
      events: [
        { seq: 5, payload: "a" },
        { seq: 7, payload: "b" },
      ],
    });
    for (const seq of [3, 7]) {
      const outcome = await engine.submit({
        batchId: `SEQ-X${seq}`,
        events: [{ seq, payload: "late" }],
      });
      t.assertEqual(outcome.status, "rejected");
      t.assertEqual(outcome.error.code, "sequence-regression");
    }
    const ok = await engine.submit({
      batchId: "SEQ-2",
      events: [{ seq: 8, payload: "next" }],
    });
    t.assertEqual(ok.status, "sealed");
  });
}
