// Concurrency acceptance: two independent connections over one shared
// store — two review pages on the same device, one sealing database —
// read the same initial chain and then interleave their submissions.
// The two legitimate batches must converge to a single continuous ordered
// chain (each batch exactly once, later receipts binding the actual
// predecessor, reopen state consistent), and an interleaved submission
// that cannot converge must not be reported sealed. Also covers
// equivalent retransmission and conflicting content across connections.

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

function batch(batchId, seqs, payloadPrefix) {
  return {
    batchId,
    events: seqs.map((seq, i) => ({ seq, payload: `${payloadPrefix}-${i}` })),
  };
}

// A pause point fires once: the first hooked call after arming blocks until
// release() is invoked, letting the test interleave two submissions at
// exact persistence stages. Later calls pass straight through.
function makePausePoint({ armed = true } = {}) {
  let enter;
  let exit;
  const entered = new Promise((resolve) => (enter = resolve));
  const proceed = new Promise((resolve) => (exit = resolve));
  const point = {
    entered,
    armed,
    release: () => exit(),
    hook: async () => {
      if (!point.armed) return;
      point.armed = false;
      enter();
      await proceed;
    },
  };
  return point;
}

// Wraps a storage adapter so the given methods run the pause hook after
// each call — one connection is slowed down while the other completes.
function withPauses(storage, hooks) {
  return new Proxy(storage, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (typeof value !== "function" || !hooks[prop]) return value;
      const hook = hooks[prop];
      return async (...args) => {
        const result = await value.apply(target, args);
        await hook();
        return result;
      };
    },
  });
}

/** Two independent connections over one shared backing, both opened. */
async function twoConnections(backing, pausesB = {}) {
  const engineA = new SealEngine(new MemoryStorage(backing));
  const engineB = new SealEngine(
    withPauses(new MemoryStorage(backing), pausesB)
  );
  await engineA.open();
  await engineB.open();
  return { engineA, engineB };
}

/** Reopen a fresh connection over the persisted backing (page reload). */
async function reopen(backing) {
  const engine = new SealEngine(new MemoryStorage(backing));
  const { recovery, chain } = await engine.open();
  return { engine, recovery, chain };
}

export function registerConcurrencyTests(t) {
  t.test(
    "concurrency: interleaved submissions converge to a single continuous chain",
    async () => {
      const backing = createMemoryBacking();
      // Connection B pauses after persisting its segment, before switching
      // the manifest; both connections observed only the genesis chain.
      const pauseB = makePausePoint();
      const { engineA, engineB } = await twoConnections(backing, {
        putSegment: pauseB.hook,
      });

      const batchA = batch("ALPHA", [1, 2, 3], "alpha");
      const batchB = batch("BRAVO", [4, 5], "bravo");

      const pendingB = engineB.submit(batchB); // intent + segment persisted, then paused
      await pauseB.entered;
      const outcomeA = await engineA.submit(batchA); // seals fully on genesis
      pauseB.release();
      const outcomeB = await pendingB; // loses the switch, rebases, seals

      // Both legitimate batches are sealed; the later receipt binds the
      // actual predecessor, not the stale genesis snapshot.
      t.assertEqual(outcomeA.status, "sealed");
      t.assertEqual(outcomeB.status, "sealed");
      t.assertEqual(outcomeA.receipt.prevDigest, GENESIS_DIGEST);
      t.assertEqual(
        outcomeB.receipt.prevDigest,
        outcomeA.receipt.digest,
        "second receipt must bind the actual predecessor"
      );

      // Close and reopen: the chain is unique, continuous and complete.
      const { engine, recovery, chain } = await reopen(backing);
      t.assertEqual(recovery.blocking.length, 0, "no blocking evidence");
      t.assertEqual(recovery.actions.length, 0, "nothing left to repair");
      t.assertEqual(chain.segments.length, 2, "each batch exactly once");

      const [segA, segB] = chain.segments;
      t.assertEqual(segA.batchId, "ALPHA");
      t.assertEqual(segB.batchId, "BRAVO");
      t.assertEqual(segA.prevDigest, GENESIS_DIGEST);
      t.assertEqual(segB.prevDigest, segA.digest, "continuous hash chain");
      t.assertEqual(chain.head, segB.digest);

      // The receipts correspond to the actual sealed segments.
      t.assertEqual(segA.digest, outcomeA.receipt.digest);
      t.assertEqual(segB.digest, outcomeB.receipt.digest);
      t.assertEqual(segB.receipt.prevDigest, segA.digest);

      // Sequence ranges are ordered and consistent.
      t.assertEqual(segA.seqStart, 1);
      t.assertEqual(segA.seqEnd, 3);
      t.assertEqual(segB.seqStart, 4);
      t.assertEqual(segB.seqEnd, 5);
      t.assert(segB.seqStart > segA.seqEnd, "monotone sequence ranges");

      // Batch mapping lists each batch exactly once, pointing at its segment.
      const manifest = backing.manifest;
      t.assertDeepEqual(Object.keys(manifest.batches).sort(), [
        "ALPHA",
        "BRAVO",
      ]);
      t.assertEqual(manifest.batches.ALPHA, segA.digest);
      t.assertEqual(manifest.batches.BRAVO, segB.digest);
      t.assertDeepEqual(manifest.segmentIds, [segA.digest, segB.digest]);

      // Digests verify against an independent SHA-256 of the canonical form.
      const expectedA = sha256Node(canonicalSegment(GENESIS_DIGEST, batchA.events));
      const expectedB = sha256Node(canonicalSegment(expectedA, batchB.events));
      t.assertEqual(segA.digest, expectedA);
      t.assertEqual(segB.digest, expectedB);

      // What the reopened page displays: recovery summary matches the chain.
      t.assertEqual(recovery.sealed, 2);
      t.assertEqual(recovery.head, segB.digest);
      t.assertEqual(backing.prepares.size, 0, "no leftover intents");

      // A later sequential submission binds to the converged head.
      const third = await engine.submit(batch("CHARLIE", [6], "charlie"));
      t.assertEqual(third.status, "sealed");
      t.assertEqual(third.receipt.prevDigest, segB.digest);
    }
  );

  t.test(
    "concurrency: a submission that cannot extend the chain is not reported sealed",
    async () => {
      const backing = createMemoryBacking();
      const pauseB = makePausePoint();
      const { engineA, engineB } = await twoConnections(backing, {
        putSegment: pauseB.hook,
      });

      // B's batch carries the earlier sequence numbers but loses the race
      // to switch the manifest.
      const earlyBatch = batch("EARLY", [1, 2], "early");
      const lateBatch = batch("LATE", [10, 11], "late");

      const pendingB = engineB.submit(earlyBatch); // intent + segment on genesis, paused
      await pauseB.entered;
      const outcomeA = await engineA.submit(lateBatch); // seals on genesis
      t.assertEqual(outcomeA.status, "sealed");
      pauseB.release();
      const outcomeB = await pendingB; // cannot converge on the new head

      t.assertEqual(
        outcomeB.status,
        "rejected",
        "a batch that cannot join the chain must not be reported sealed"
      );
      t.assertEqual(outcomeB.error.code, "sequence-regression");

      // Reopen: only the genuinely sealed batch is on the chain; the
      // refused submission left no pending intent and no blocking evidence.
      const { recovery, chain } = await reopen(backing);
      t.assertEqual(chain.segments.length, 1);
      t.assertEqual(chain.segments[0].batchId, "LATE");
      t.assertEqual(chain.segments[0].prevDigest, GENESIS_DIGEST);
      t.assertEqual(chain.head, chain.segments[0].digest);
      t.assertEqual(recovery.blocking.length, 0);
      t.assertEqual(backing.prepares.size, 0, "refused intent swept");
    }
  );

  t.test(
    "concurrency: identical retransmission from both connections returns the first final receipt",
    async () => {
      const backing = createMemoryBacking();
      const pauseB = makePausePoint();
      const { engineA, engineB } = await twoConnections(backing, {
        putSegment: pauseB.hook,
      });

      const same = batch("SAME", [1, 2], "same");
      const pendingB = engineB.submit(same); // records the intent, pauses
      await pauseB.entered;
      const outcomeA = await engineA.submit(same); // fulfills the recorded intent
      pauseB.release();
      const outcomeB = await pendingB; // already published: original receipt

      t.assertEqual(outcomeA.status, "sealed");
      t.assertEqual(outcomeB.status, "duplicate");
      t.assertDeepEqual(
        outcomeB.receipt,
        outcomeA.receipt,
        "both connections receive the first final receipt"
      );

      const { recovery, chain } = await reopen(backing);
      t.assertEqual(chain.segments.length, 1, "batch appended exactly once");
      t.assertEqual(chain.segments[0].digest, outcomeA.receipt.digest);
      t.assertEqual(recovery.blocking.length, 0);
      t.assertEqual(backing.prepares.size, 0);
    }
  );

  t.test(
    "concurrency: conflicting content across connections keeps existing evidence",
    async () => {
      const backing = createMemoryBacking();
      // B pauses on its very first manifest read (armed only after open),
      // so A seals its content under the shared batch id first.
      const pauseB = makePausePoint({ armed: false });
      const { engineA, engineB } = await twoConnections(backing, {
        getManifest: pauseB.hook,
      });
      pauseB.armed = true;

      const pendingB = engineB.submit(batch("CONF", [1, 2], "incoming"));
      await pauseB.entered; // B holds a stale genesis snapshot
      const outcomeA = await engineA.submit(batch("CONF", [1, 2], "existing"));
      t.assertEqual(outcomeA.status, "sealed");
      pauseB.release();
      const outcomeB = await pendingB;

      t.assertEqual(outcomeB.status, "conflict");
      t.assertEqual(outcomeB.conflict.reason, "published-content-mismatch");
      t.assertDeepEqual(outcomeB.conflict.existingReceipt, outcomeA.receipt);

      // The existing evidence is untouched; the conflict is recorded.
      const conflicts = await engineA.conflicts();
      t.assertEqual(conflicts.length, 1);
      t.assertEqual(conflicts[0].batchId, "CONF");

      const { recovery, chain } = await reopen(backing);
      t.assertEqual(chain.segments.length, 1);
      t.assertEqual(chain.segments[0].digest, outcomeA.receipt.digest);
      t.assertEqual(chain.segments[0].events[0].payload, "existing-0");
      t.assertEqual(recovery.blocking.length, 0);
      t.assertEqual(backing.prepares.size, 0, "loser intent swept by recovery");
    }
  );
}
