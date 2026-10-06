import {
  GENESIS_DIGEST,
  contentHashOf,
  segmentDigestOf,
} from "./canonical.js";
import { validateBatch } from "./validate.js";

// Stages of the persistence protocol. A drill may request a simulated
// crash immediately after any of these stages has been persisted:
//   afterPrepare  — prepare intent written, segment not yet written
//   afterSegment  — immutable segment written, manifest not yet switched
//   afterManifest — active manifest switched (batch fully published)
export const CRASH_STAGES = ["afterPrepare", "afterSegment", "afterManifest"];

export class CrashError extends Error {
  constructor(stage) {
    super(`simulated crash after stage: ${stage}`);
    this.name = "CrashError";
    this.stage = stage;
  }
}

const MAX_CONFLICT_LOG = 100;

// Bounds on the compare-and-swap retry loops. Two reviewers on one device
// resolve in one or two attempts; the bound only guards against pathological
// contention, in which case the submission is blocked rather than sealed.
const MAX_SUBMIT_ATTEMPTS = 8;
const MAX_RECOVERY_ATTEMPTS = 8;

function emptyManifest() {
  return {
    id: "active",
    head: GENESIS_DIGEST,
    segmentIds: [],
    batches: {},
    version: 0,
  };
}

// The immutable segment (and its receipt) addressed by an intent's
// expectedDigest. Rebuilt identically from the recorded intent, so a
// repeated write is idempotent.
function segmentFromIntent(intent) {
  return {
    digest: intent.expectedDigest,
    batchId: intent.batchId,
    contentHash: intent.contentHash,
    prevDigest: intent.prevDigest,
    seqStart: intent.events[0].seq,
    seqEnd: intent.events[intent.events.length - 1].seq,
    events: intent.events,
    receipt: {
      batchId: intent.batchId,
      digest: intent.expectedDigest,
      prevDigest: intent.prevDigest,
      seqStart: intent.events[0].seq,
      seqEnd: intent.events[intent.events.length - 1].seq,
      count: intent.events.length,
    },
  };
}

// The active manifest after appending one segment to the given snapshot.
function nextManifestAfter(manifest, segment) {
  return {
    ...manifest,
    segmentIds: [...manifest.segmentIds, segment.digest],
    batches: { ...manifest.batches, [segment.batchId]: segment.digest },
    head: segment.digest,
    version: manifest.version + 1,
  };
}

/**
 * Sealing engine. Operates on a storage adapter with this interface:
 *   getPrepare(batchId) / putPrepare(rec) / deletePrepare(batchId) / listPrepares()
 *   getSegment(digest) / putSegment(rec)
 *   getManifest() / switchManifest(expectedVersion, rec)
 *   getMeta(key) / setMeta(key, value)
 *
 * Persistence protocol for a new batch (each step its own durable write):
 *   1. prepare intent  (prepares store, keyed by batchId)
 *   2. immutable segment (segments store, keyed by its own digest)
 *   3. active manifest switch (manifest store, single "active" record)
 *
 * The manifest switch is an atomic compare-and-swap on the version read
 * with the snapshot the intent was built against. Two connections sharing
 * one store (two pages on one device) can therefore never both switch the
 * manifest from the same snapshot: the loser reloads, rebases its intent
 * onto the fresh head and retries, so interleaved submissions converge to
 * a single continuous chain — or, when the batch can no longer extend the
 * sealed record, the submission is refused instead of reported sealed.
 *
 * Only segments pointed to by the active manifest whose digest, predecessor
 * and sequence numbers verify continuously belong to the sealed record.
 */
export class SealEngine {
  #storage;
  #failpoints;

  constructor(storage, options = {}) {
    this.#storage = storage;
    this.#failpoints = options.failpoints ?? {};
  }

  /** Open the store: scan and repair residual state, then return the chain. */
  async open() {
    const recovery = await this.#recover();
    const chain = await this.sealedChain();
    return { recovery, chain };
  }

  /** Segments currently belonging to the sealed record. */
  async sealedChain() {
    const manifest = await this.#loadManifest();
    const segments = [];
    for (const digest of manifest.segmentIds) {
      const segment = await this.#storage.getSegment(digest);
      if (segment) segments.push(segment);
    }
    return { head: manifest.head, version: manifest.version, segments };
  }

  async conflicts() {
    return (await this.#storage.getMeta("conflicts")) ?? [];
  }

  async lastRecovery() {
    return (await this.#storage.getMeta("lastRecovery")) ?? null;
  }

  /**
   * Submit a batch. Outcomes:
   *   { status: "sealed",  receipt }  — appended to the chain
   *   { status: "duplicate", receipt }— same id + same content: original receipt
   *   { status: "conflict", conflict }— same id, different content: evidence kept
   *   { status: "rejected", error }   — validation / sequencing failure
   *   { status: "blocked",  error }   — intent can no longer be fulfilled
   */
  async submit(batch) {
    const validation = validateBatch(batch);
    if (!validation.ok) {
      return { status: "rejected", error: validation };
    }
    const events = batch.events.map((e) => ({ seq: e.seq, payload: e.payload }));
    const contentHash = await contentHashOf(events);
    const manifest = await this.#loadManifest();

    // Already published under this batch id?
    const published = await this.#publishedOutcome(
      manifest,
      batch.batchId,
      contentHash
    );
    if (published) return published;

    // A pending (unpublished) intent under this batch id?
    const intent = await this.#storage.getPrepare(batch.batchId);
    if (intent) {
      if (intent.contentHash !== contentHash) {
        const conflict = {
          reason: "pending-intent-content-mismatch",
          batchId: batch.batchId,
          incomingContentHash: contentHash,
          existingContentHash: intent.contentHash,
          existingReceipt: null,
          at: new Date().toISOString(),
        };
        await this.#recordConflict(conflict);
        return { status: "conflict", conflict };
      }
      // Same content: resume the established intent, do not start a new one.
      return this.#fulfillRecordedIntent(intent, manifest);
    }

    return this.#sealNewBatch(batch.batchId, events, contentHash, manifest);
  }

  // ---- persistence protocol --------------------------------------------

  // Outcome for a batch id that is already present in the manifest's batch
  // map: identical retransmission hands back the original receipt; different
  // content keeps the existing evidence and records an explicit conflict.
  // Returns null while the id is unpublished.
  async #publishedOutcome(manifest, batchId, contentHash) {
    if (!Object.hasOwn(manifest.batches, batchId)) return null;
    const existing = await this.#storage.getSegment(manifest.batches[batchId]);
    if (existing && existing.contentHash === contentHash) {
      // Identical retransmission: hand back the original receipt, append nothing.
      return { status: "duplicate", receipt: existing.receipt };
    }
    // Different content under a published id: keep existing evidence, report.
    const conflict = {
      reason: "published-content-mismatch",
      batchId,
      incomingContentHash: contentHash,
      existingContentHash: existing?.contentHash ?? null,
      existingReceipt: existing?.receipt ?? null,
      at: new Date().toISOString(),
    };
    await this.#recordConflict(conflict);
    return { status: "conflict", conflict };
  }

  // Seal a batch with no recorded intent. The manifest switch is a
  // compare-and-swap; whenever a concurrent connection switches first, the
  // intent is rebased onto the fresh head and retried, so both legitimate
  // batches converge to one continuous chain. A batch that can no longer
  // extend the sealed record is refused — never reported sealed.
  async #sealNewBatch(batchId, events, contentHash, manifest) {
    let intent = null;
    for (let attempt = 0; attempt < MAX_SUBMIT_ATTEMPTS; attempt += 1) {
      // A concurrent connection may have published this batch id meanwhile.
      const published = await this.#publishedOutcome(manifest, batchId, contentHash);
      if (published) {
        if (intent && published.status === "duplicate") {
          // Our own pending intent is fulfilled by the published segment.
          await this.#storage.deletePrepare(batchId);
        }
        return published;
      }

      // Sequence numbers must extend the sealed record monotonically.
      const { lastSeqEnd } = await this.#headState(manifest);
      if (lastSeqEnd !== null && events[0].seq <= lastSeqEnd) {
        // Cannot converge on the current head: sweep the intent this
        // submission persisted and refuse — a sealed report would be a lie.
        if (intent) await this.#storage.deletePrepare(batchId);
        return {
          status: "rejected",
          error: {
            code: "sequence-regression",
            detail: `first seq ${events[0].seq} must be greater than the sealed last seq ${lastSeqEnd}`,
          },
        };
      }

      intent = {
        batchId,
        contentHash,
        events,
        prevDigest: manifest.head,
        expectedDigest: await segmentDigestOf(manifest.head, events),
        createdAt: intent?.createdAt ?? Date.now(),
      };

      // Stage 1: prepare intent.
      await this.#storage.putPrepare(intent);
      await this.#maybeCrash("afterPrepare");

      // Stage 2: immutable segment, content-addressed by its own digest.
      const segment = segmentFromIntent(intent);
      await this.#storage.putSegment(segment);
      await this.#maybeCrash("afterSegment");

      // Stage 3: switch the active manifest, but only if it is still the
      // snapshot this intent was built against.
      const switched = await this.#storage.switchManifest(
        manifest.version,
        nextManifestAfter(manifest, segment)
      );
      if (!switched) {
        // A concurrent connection moved the head: rebase on the fresh
        // manifest and retry instead of forcing a discontinuous chain.
        manifest = await this.#loadManifest();
        continue;
      }
      await this.#maybeCrash("afterManifest");

      // Best-effort intent cleanup; a leftover fulfilled intent is swept by
      // recovery on the next open.
      await this.#storage.deletePrepare(batchId);
      return { status: "sealed", receipt: segment.receipt };
    }
    return {
      status: "blocked",
      error: {
        code: "manifest-contention",
        detail:
          "the active manifest kept switching; the submission could not converge",
      },
    };
  }

  // Fulfill an intent recorded by an earlier session. The recorded intent
  // binds its batch to exactly one outcome — the segment addressed by its
  // expectedDigest — so it is fulfilled exactly as recorded or blocked,
  // never silently rebased onto a different predecessor.
  async #fulfillRecordedIntent(intent, manifest) {
    if (intent.prevDigest !== manifest.head) {
      return {
        status: "blocked",
        error: {
          code: "intent-predecessor-stale",
          detail:
            "the recorded intent predecessor no longer matches the active head",
        },
      };
    }
    const { lastSeqEnd } = await this.#headState(manifest);
    if (lastSeqEnd !== null && intent.events[0].seq <= lastSeqEnd) {
      return {
        status: "rejected",
        error: {
          code: "sequence-regression",
          detail: `first seq ${intent.events[0].seq} must be greater than the sealed last seq ${lastSeqEnd}`,
        },
      };
    }

    // Stage 2: the segment may already exist (crash after its write); the
    // rewrite is idempotent because the digest is content-addressed.
    const segment = segmentFromIntent(intent);
    await this.#storage.putSegment(segment);
    await this.#maybeCrash("afterSegment");

    // Stage 3: switch the active manifest.
    const switched = await this.#storage.switchManifest(
      manifest.version,
      nextManifestAfter(manifest, segment)
    );
    if (!switched) {
      // The head moved under us. If a concurrent connection fulfilled this
      // very intent, the batch is published and the original receipt is the
      // answer; otherwise the recorded intent no longer fits and must not
      // be forced onto the chain.
      const fresh = await this.#loadManifest();
      const published = await this.#publishedOutcome(
        fresh,
        intent.batchId,
        intent.contentHash
      );
      if (published) return published;
      return {
        status: "blocked",
        error: {
          code: "intent-predecessor-stale",
          detail:
            "the recorded intent predecessor no longer matches the active head",
        },
      };
    }
    await this.#maybeCrash("afterManifest");

    await this.#storage.deletePrepare(intent.batchId);
    return { status: "sealed", receipt: segment.receipt };
  }

  async #maybeCrash(stage) {
    if (this.#failpoints[stage]) {
      throw new CrashError(stage);
    }
  }

  // ---- recovery ----------------------------------------------------------

  async #recover() {
    for (let attempt = 0; attempt < MAX_RECOVERY_ATTEMPTS; attempt += 1) {
      const report = await this.#recoverPass();
      if (report) {
        await this.#storage.setMeta("lastRecovery", report);
        return report;
      }
      // A concurrent connection switched the active manifest mid-scan and
      // nothing was applied: rescan against the fresh state.
    }
    // Contended beyond the attempt bound: report the verified state without
    // applying repairs; the next open completes them.
    const report = await this.#verifiedStateReport();
    await this.#storage.setMeta("lastRecovery", report);
    return report;
  }

  // One recovery pass. Computes every repair from one manifest snapshot and
  // applies them with a single compare-and-swap; returns null (nothing was
  // applied) when the snapshot was switched under us by another connection.
  async #recoverPass() {
    const actions = [];
    const blocking = [];
    const deletions = []; // intent sweep, applied after the manifest switch
    const loaded = await this.#loadManifest();

    // 1. Verify the manifest-pointed chain; keep the longest valid prefix.
    let prevDigest = GENESIS_DIGEST;
    let lastSeqEnd = null;
    const validIds = [];
    const validBatches = {};
    for (const digest of loaded.segmentIds) {
      const segment = await this.#storage.getSegment(digest);
      const problem = await this.#verifySegmentLink(
        segment,
        digest,
        prevDigest,
        lastSeqEnd
      );
      if (problem) {
        blocking.push({
          code: problem.code,
          digest,
          batchId: segment?.batchId ?? null,
          detail: problem.detail,
        });
        break; // first blocking evidence: everything from here on is excluded
      }
      validIds.push(digest);
      validBatches[segment.batchId] = digest;
      prevDigest = digest;
      lastSeqEnd = segment.seqEnd;
    }

    let manifest = loaded;
    let changed = false;
    if (validIds.length !== loaded.segmentIds.length) {
      const dropped = loaded.segmentIds.slice(validIds.length);
      manifest = {
        ...loaded,
        segmentIds: validIds,
        batches: validBatches,
        head: prevDigest,
        version: loaded.version + 1,
      };
      changed = true;
      actions.push({
        action: "chain-truncated",
        kept: validIds.length,
        droppedDigests: dropped,
      });
    }

    // 2. Scan leftover prepare intents.
    const prepares = await this.#storage.listPrepares();
    prepares.sort(
      (a, b) => a.createdAt - b.createdAt || a.batchId.localeCompare(b.batchId)
    );
    for (const intent of prepares) {
      if (Object.hasOwn(manifest.batches, intent.batchId)) {
        // Already published: the intent was fulfilled before the crash.
        // Never append a published segment a second time.
        deletions.push(intent.batchId);
        actions.push({
          action: "fulfilled-intent-cleaned",
          batchId: intent.batchId,
          digest: manifest.batches[intent.batchId],
        });
        continue;
      }

      const segment = await this.#storage.getSegment(intent.expectedDigest);
      if (!segment) {
        // Segment was never persisted: incomplete intent, must not enter the chain.
        deletions.push(intent.batchId);
        actions.push({
          action: "intent-discarded-incomplete",
          batchId: intent.batchId,
        });
        continue;
      }

      const problem = await this.#verifyIntentSegment(
        intent,
        segment,
        manifest,
        lastSeqEnd
      );
      if (problem) {
        // Corrupt or inconsistent prepared segment: keep it as orphan
        // evidence, but it must not enter the chain.
        deletions.push(intent.batchId);
        blocking.push({
          code: problem.code,
          batchId: intent.batchId,
          digest: intent.expectedDigest,
          detail: problem.detail,
        });
        actions.push({
          action: "prepared-segment-rejected",
          batchId: intent.batchId,
          digest: intent.expectedDigest,
          reason: problem.code,
        });
        continue;
      }

      // Complete and consistent with the established intent: publish it as
      // the unique recovery outcome for this batch.
      manifest = {
        ...manifest,
        segmentIds: [...manifest.segmentIds, segment.digest],
        batches: { ...manifest.batches, [segment.batchId]: segment.digest },
        head: segment.digest,
        version: manifest.version + 1,
      };
      changed = true;
      deletions.push(intent.batchId);
      lastSeqEnd = segment.seqEnd;
      actions.push({
        action: "segment-published-from-intent",
        batchId: intent.batchId,
        digest: segment.digest,
        seqStart: segment.seqStart,
        seqEnd: segment.seqEnd,
      });
    }

    // 3. Apply the repairs: one atomic manifest switch, then the intent
    //    sweep. A lost race discards this pass before anything was applied.
    if (changed) {
      const switched = await this.#storage.switchManifest(
        loaded.version,
        manifest
      );
      if (!switched) return null;
    }
    for (const batchId of deletions) {
      await this.#storage.deletePrepare(batchId);
    }

    return {
      at: new Date().toISOString(),
      actions,
      blocking,
      firstBlocking: blocking[0] ?? null,
      sealed: manifest.segmentIds.length,
      head: manifest.head,
    };
  }

  // Read-only report of the verifiable chain, used when recovery is too
  // contended to apply repairs.
  async #verifiedStateReport() {
    const manifest = await this.#loadManifest();
    const blocking = [];
    let prevDigest = GENESIS_DIGEST;
    let lastSeqEnd = null;
    let sealed = 0;
    for (const digest of manifest.segmentIds) {
      const segment = await this.#storage.getSegment(digest);
      const problem = await this.#verifySegmentLink(
        segment,
        digest,
        prevDigest,
        lastSeqEnd
      );
      if (problem) {
        blocking.push({
          code: problem.code,
          digest,
          batchId: segment?.batchId ?? null,
          detail: problem.detail,
        });
        break;
      }
      sealed += 1;
      prevDigest = digest;
      lastSeqEnd = segment.seqEnd;
    }
    return {
      at: new Date().toISOString(),
      actions: [{ action: "recovery-deferred", reason: "manifest-contention" }],
      blocking,
      firstBlocking: blocking[0] ?? null,
      sealed,
      head: prevDigest,
    };
  }

  // Verify one manifest-linked segment against its expected predecessor.
  async #verifySegmentLink(segment, digest, expectedPrev, lastSeqEnd) {
    if (!segment) {
      return {
        code: "segment-missing",
        detail: `manifest points to missing segment ${digest}`,
      };
    }
    if (segment.digest !== digest) {
      return {
        code: "segment-key-mismatch",
        detail: `stored digest ${segment.digest} differs from manifest pointer ${digest}`,
      };
    }
    if (!Array.isArray(segment.events) || segment.events.length === 0) {
      return { code: "segment-shape-invalid", detail: "segment has no events" };
    }
    for (let i = 0; i < segment.events.length; i += 1) {
      const event = segment.events[i];
      if (!Number.isSafeInteger(event.seq) || typeof event.payload !== "string") {
        return {
          code: "segment-shape-invalid",
          detail: `events[${i}] is malformed`,
        };
      }
      if (i > 0 && event.seq <= segment.events[i - 1].seq) {
        return {
          code: "segment-shape-invalid",
          detail: "events are not strictly increasing",
        };
      }
    }
    if (
      segment.seqStart !== segment.events[0].seq ||
      segment.seqEnd !== segment.events[segment.events.length - 1].seq
    ) {
      return {
        code: "segment-shape-invalid",
        detail: "seqStart/seqEnd do not match the event range",
      };
    }
    if (segment.prevDigest !== expectedPrev) {
      return {
        code: "predecessor-mismatch",
        detail: `expected predecessor ${expectedPrev}, found ${segment.prevDigest}`,
      };
    }
    if (lastSeqEnd !== null && segment.seqStart <= lastSeqEnd) {
      return {
        code: "sequence-overlap",
        detail: `seqStart ${segment.seqStart} does not follow previous seqEnd ${lastSeqEnd}`,
      };
    }
    const recomputed = await segmentDigestOf(segment.prevDigest, segment.events);
    if (recomputed !== segment.digest) {
      return {
        code: "segment-digest-mismatch",
        detail: "recomputed digest does not match the stored digest",
      };
    }
    const contentHash = await contentHashOf(segment.events);
    if (contentHash !== segment.contentHash) {
      return {
        code: "segment-content-mismatch",
        detail: "recomputed content hash does not match the stored content hash",
      };
    }
    return null;
  }

  // Verify a leftover prepared segment against its recorded intent and the
  // current head. Only the segment addressed by the intent's expectedDigest
  // can ever be published — the unique recovery outcome.
  async #verifyIntentSegment(intent, segment, manifest, lastSeqEnd) {
    if (segment.batchId !== intent.batchId) {
      return {
        code: "intent-segment-batch-mismatch",
        detail: `segment belongs to batch ${segment.batchId}, intent is ${intent.batchId}`,
      };
    }
    if (segment.prevDigest !== intent.prevDigest) {
      return {
        code: "intent-predecessor-mismatch",
        detail: "segment predecessor differs from the recorded intent",
      };
    }
    if (segment.prevDigest !== manifest.head) {
      return {
        code: "intent-predecessor-stale",
        detail: "intent predecessor does not match the current head",
      };
    }
    const link = await this.#verifySegmentLink(
      segment,
      intent.expectedDigest,
      intent.prevDigest,
      lastSeqEnd
    );
    if (link) return link;
    if (segment.contentHash !== intent.contentHash) {
      return {
        code: "intent-content-mismatch",
        detail: "segment content hash differs from the recorded intent",
      };
    }
    return null;
  }

  // ---- helpers -----------------------------------------------------------

  async #loadManifest() {
    return (await this.#storage.getManifest()) ?? emptyManifest();
  }

  async #headState(manifest) {
    if (manifest.segmentIds.length === 0) return { lastSeqEnd: null };
    const lastDigest = manifest.segmentIds[manifest.segmentIds.length - 1];
    const last = await this.#storage.getSegment(lastDigest);
    return { lastSeqEnd: last ? last.seqEnd : null };
  }

  async #recordConflict(conflict) {
    const log = (await this.#storage.getMeta("conflicts")) ?? [];
    log.push(conflict);
    if (log.length > MAX_CONFLICT_LOG) {
      log.splice(0, log.length - MAX_CONFLICT_LOG);
    }
    await this.#storage.setMeta("conflicts", log);
  }
}
