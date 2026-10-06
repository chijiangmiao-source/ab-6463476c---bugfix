// Concurrency acceptance: two independent connections (two review pages on
// one device) read the same initial chain and then interleave their submits.
// The batches must converge to a single continuous chain — each batch
// exactly once, later receipts bound to the actual predecessor — and any
// interleave that cannot form that chain must not be reported as sealed.

import { SealEngine, CrashError } from "../../core/engine.js";
import {
  MemoryStorage,
  createMemoryBacking,
} from "../../core/memory-storage.js";
import { GENESIS_DIGEST } from "../../core/canonical.js";

function batch(batchId, seqs, payloadPrefix = "event") {
  return {
    batchId,
    events: seqs.map((seq, i) => ({ seq, payload: `${payloadPrefix}-${i}` })),
  };
}

// A connection that pauses inside the next call to a chosen method until the
// test releases it — forces a deterministic interleave against a second,
// ungated connection over the same persisted backing.
class GatedStorage extends MemoryStorage {
  #holds = new Map();

  pauseNext(method) {
    let markEntered;
    let release;
    const gate = {
      entered: new Promise((resolve) => {
        markEntered = resolve;
      }),
      release: () => release(),
    };
    const hold = new Promise((resolve) => {
      release = resolve;
    });
    this.#holds.set(method, { markEntered, hold });
    return gate;
  }

  async getManifest() {
    const manifest = await super.getManifest();
    const gate = this.#holds.get("getManifest");
    if (gate) {
      this.#holds.delete("getManifest");
      gate.markEntered();
      await gate.hold;
    }
    return manifest;
  }
}

// Two opened engines on independent connections over one shared backing,
// with connection B gated for interleave control.
async function twoConnections() {
  const backing = createMemoryBacking();
  const engineA = new SealEngine(new MemoryStorage(backing));
  const storageB = new GatedStorage(backing);
  const engineB = new SealEngine(storageB);
  await engineA.open();
  await engineB.open();
  return { backing, engineA, engineB, storageB };
}

async function reopen(backing) {
  const engine = new SealEngine(new MemoryStorage(backing));
  const { recovery, chain } = await engine.open();
  return { engine, recovery, chain };
}

function assertContinuous(t, chain) {
  t.assert(chain.segments.length > 0, "chain must not be empty");
  t.assertEqual(
    chain.segments[0].prevDigest,
    GENESIS_DIGEST,
    "first segment binds the genesis digest"
  );
  for (let i = 1; i < chain.segments.length; i += 1) {
    t.assertEqual(
      chain.segments[i].prevDigest,
      chain.segments[i - 1].digest,
      `segment ${i} must bind its actual predecessor`
    );
    t.assert(
      chain.segments[i].seqStart > chain.segments[i - 1].seqEnd,
      `segment ${i} seqs must follow the previous segment`
    );
  }
  t.assertEqual(
    chain.head,
    chain.segments[chain.segments.length - 1].digest,
    "head is the last segment"
  );
}

export function registerConcurrencyTests(t) {
  t.test(
    "concurrency: interleaved submits converge to one continuous chain",
    async () => {
      const { backing, engineA, engineB, storageB } = await twoConnections();
      const batchA = batch("REVIEW-A", [1, 2, 3], "alpha");
      const batchB = batch("REVIEW-B", [4, 5, 6], "bravo");

      // Both connections read the same initial (genesis) chain; B stalls
      // inside its read, A commits fully, then B resumes and must converge
      // onto A's head instead of publishing a second genesis-bound segment.
      const gate = storageB.pauseNext("getManifest");
      const submitB = engineB.submit(batchB);
      await gate.entered;
      const outcomeA = await engineA.submit(batchA);
      gate.release();
      const outcomeB = await submitB;

      t.assertEqual(outcomeA.status, "sealed");
      t.assertEqual(outcomeA.receipt.prevDigest, GENESIS_DIGEST);
      t.assertEqual(outcomeB.status, "sealed");
      t.assertEqual(
        outcomeB.receipt.prevDigest,
        outcomeA.receipt.digest,
        "later receipt binds the actual predecessor, not the stale genesis read"
      );

      // Close and reopen: a fresh connection recovers and must find a
      // unique, continuous chain with each batch exactly once.
      const { recovery, chain } = await reopen(backing);
      t.assertEqual(chain.segments.length, 2);
      t.assertDeepEqual(
        chain.segments.map((s) => s.batchId),
        ["REVIEW-A", "REVIEW-B"]
      );
      assertContinuous(t, chain);
      t.assertDeepEqual(
        chain.segments.map((s) => [s.seqStart, s.seqEnd]),
        [
          [1, 3],
          [4, 6],
        ]
      );
      t.assertEqual(chain.segments[0].digest, outcomeA.receipt.digest);
      t.assertEqual(chain.segments[1].digest, outcomeB.receipt.digest);
      t.assertEqual(recovery.blocking.length, 0, "no blocking evidence");
      t.assertEqual(recovery.actions.length, 0, "nothing to repair");
      t.assertEqual(backing.manifest.version, 2, "one manifest switch per batch");
      t.assertDeepEqual(backing.manifest.batches, {
        "REVIEW-A": outcomeA.receipt.digest,
        "REVIEW-B": outcomeB.receipt.digest,
      });
      t.assertEqual(backing.prepares.size, 0, "no leftover intents");

      // The receipts correspond to the sealed record: retransmitting either
      // batch returns its first final receipt and appends nothing.
      const { engine: reopened } = await reopen(backing);
      const againA = await reopened.submit(batchA);
      t.assertEqual(againA.status, "duplicate");
      t.assertDeepEqual(againA.receipt, outcomeA.receipt);
      const againB = await reopened.submit(batchB);
      t.assertEqual(againB.status, "duplicate");
      t.assertDeepEqual(againB.receipt, outcomeB.receipt);
      t.assertEqual((await reopened.sealedChain()).segments.length, 2);
    }
  );

  t.test(
    "concurrency: same id + same content interleaved seals exactly once",
    async () => {
      const { backing, engineA, engineB, storageB } = await twoConnections();
      const shared = batch("SAME-1", [1, 2], "same");

      const gate = storageB.pauseNext("getManifest");
      const submitB = engineB.submit(shared);
      await gate.entered;
      const outcomeA = await engineA.submit(shared);
      gate.release();
      const outcomeB = await submitB;

      t.assertEqual(outcomeA.status, "sealed");
      t.assertEqual(outcomeB.status, "duplicate");
      t.assertDeepEqual(outcomeB.receipt, outcomeA.receipt);

      const { chain } = await reopen(backing);
      t.assertEqual(chain.segments.length, 1);
      t.assertEqual(chain.segments[0].digest, outcomeA.receipt.digest);
      t.assertEqual(backing.prepares.size, 0, "recovery swept the leftover intent");
    }
  );

  t.test(
    "concurrency: same id + different content interleaved conflicts, evidence kept",
    async () => {
      const { backing, engineA, engineB, storageB } = await twoConnections();
      const original = batch("CLASH-1", [1, 2], "original");
      const tampered = batch("CLASH-1", [1, 2], "tampered");

      const gate = storageB.pauseNext("getManifest");
      const submitB = engineB.submit(tampered);
      await gate.entered;
      const outcomeA = await engineA.submit(original);
      gate.release();
      const outcomeB = await submitB;

      t.assertEqual(outcomeA.status, "sealed");
      t.assertEqual(outcomeB.status, "conflict");
      t.assertEqual(outcomeB.conflict.reason, "published-content-mismatch");
      t.assertDeepEqual(outcomeB.conflict.existingReceipt, outcomeA.receipt);

      const { engine, chain } = await reopen(backing);
      t.assertEqual(chain.segments.length, 1);
      t.assertEqual(chain.segments[0].events[0].payload, "original-0");
      const conflicts = await engine.conflicts();
      t.assertEqual(conflicts.length, 1);
      t.assertEqual(conflicts[0].batchId, "CLASH-1");
    }
  );

  t.test(
    "concurrency: overlapping seqs cannot both seal",
    async () => {
      const { backing, engineA, engineB, storageB } = await twoConnections();

      // B's first seq collides with A's range; once A is sealed, B can no
      // longer extend the record and must not be reported sealed.
      const gate = storageB.pauseNext("getManifest");
      const submitB = engineB.submit(batch("OVER-B", [3, 4, 5], "b"));
      await gate.entered;
      const outcomeA = await engineA.submit(batch("OVER-A", [1, 2, 3], "a"));
      gate.release();
      const outcomeB = await submitB;

      t.assertEqual(outcomeA.status, "sealed");
      t.assertEqual(outcomeB.status, "rejected");
      t.assertEqual(outcomeB.error.code, "sequence-regression");

      const { recovery, chain } = await reopen(backing);
      t.assertEqual(chain.segments.length, 1);
      t.assertEqual(chain.segments[0].batchId, "OVER-A");
      t.assert(
        !chain.segments.some((s) => s.batchId === "OVER-B"),
        "the unsealable batch never entered the chain"
      );
      t.assertEqual(
        recovery.firstBlocking?.code,
        "intent-predecessor-stale",
        "the stale prepared segment is kept as evidence, not linked"
      );
    }
  );

  t.test(
    "concurrency: crash mid-interleave keeps the stale segment out of the chain",
    async () => {
      const backing = createMemoryBacking();
      const engineA = new SealEngine(new MemoryStorage(backing));
      await engineA.open();

      // Connection B prepares against the genesis head and crashes after
      // its segment write; connection A then seals its own batch on top of
      // the same genesis head.
      const drillB = new SealEngine(new MemoryStorage(backing), {
        failpoints: { afterSegment: true },
      });
      await drillB.open();
      const batchB = batch("CRASH-B", [4, 5], "b");
      try {
        await drillB.submit(batchB);
        throw new Error("expected a CrashError");
      } catch (err) {
        if (!(err instanceof CrashError)) throw err;
      }
      const outcomeA = await engineA.submit(batch("CRASH-A", [1, 2, 3], "a"));
      t.assertEqual(outcomeA.status, "sealed");

      // Reopen: B's prepared segment is stale relative to the sealed head
      // and must stay out of the chain.
      const { engine, recovery, chain } = await reopen(backing);
      t.assertEqual(chain.segments.length, 1);
      t.assertEqual(chain.segments[0].batchId, "CRASH-A");
      t.assertEqual(recovery.firstBlocking?.code, "intent-predecessor-stale");
      t.assertEqual(recovery.firstBlocking?.batchId, "CRASH-B");

      // Resubmitted after recovery, B binds to the actual head and seals.
      const retry = await engine.submit(batchB);
      t.assertEqual(retry.status, "sealed");
      t.assertEqual(retry.receipt.prevDigest, outcomeA.receipt.digest);
      const after = await engine.sealedChain();
      t.assertEqual(after.segments.length, 2);
      assertContinuous(t, after);
    }
  );

  t.test(
    "concurrency: ungated simultaneous submits always converge",
    async () => {
      // No interleave control: whatever the scheduling, the outcome must be
      // one continuous chain. B's seqs follow A's, so B always seals; A
      // seals iff it switches the manifest first, otherwise it is rejected
      // rather than sealed off a stale chain.
      for (let round = 0; round < 10; round += 1) {
        const backing = createMemoryBacking();
        const engineA = new SealEngine(new MemoryStorage(backing));
        const engineB = new SealEngine(new MemoryStorage(backing));
        await engineA.open();
        await engineB.open();
        const batchA = batch("SIM-A", [1, 2, 3], "a");
        const batchB = batch("SIM-B", [4, 5, 6], "b");

        const [outcomeA, outcomeB] = await Promise.all([
          engineA.submit(batchA),
          engineB.submit(batchB),
        ]);

        t.assertEqual(outcomeB.status, "sealed", "B must always seal");
        const { chain } = await reopen(backing);
        if (outcomeA.status === "sealed") {
          t.assertEqual(chain.segments.length, 2);
          t.assertDeepEqual(
            chain.segments.map((s) => s.batchId),
            ["SIM-A", "SIM-B"]
          );
          assertContinuous(t, chain);
          t.assertEqual(chain.segments[0].digest, outcomeA.receipt.digest);
          t.assertEqual(chain.segments[1].digest, outcomeB.receipt.digest);
        } else {
          t.assertEqual(outcomeA.status, "rejected");
          t.assertEqual(outcomeA.error.code, "sequence-regression");
          t.assertEqual(chain.segments.length, 1);
          t.assertEqual(chain.segments[0].batchId, "SIM-B");
          t.assertEqual(chain.segments[0].digest, outcomeB.receipt.digest);
        }
      }
    }
  );
}
