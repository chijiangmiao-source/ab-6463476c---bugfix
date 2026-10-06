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

// How many times one submit may rebuild its intent against a freshly read
// manifest before giving up. Each retry means another connection switched
// the manifest first; two reviewers interleaving on one device converge on
// the second attempt.
const MAX_SUBMIT_ATTEMPTS = 8;

function emptyManifest() {
  return {
    id: "active",
    head: GENESIS_DIGEST,
    segmentIds: [],
    batches: {},
    version: 0,
  };
}

/**
 * Sealing engine. Operates on a storage adapter with this interface:
 *   getPrepare(batchId) / putPrepare(rec) / deletePrepare(batchId) / listPrepares()
 *   getSegment(digest) / putSegment(rec)
 *   getManifest() / putManifest(rec) / switchManifest(rec, expectedVersion)
 *   getMeta(key) / setMeta(key, value)
 *
 * Persistence protocol for a new batch (each step its own durable write):
 *   1. prepare intent  (prepares store, keyed by batchId)
 *   2. immutable segment (segments store, keyed by its own digest)
 *   3. active manifest switch (manifest store, single "active" record)
 *
 * Step 3 is a compare-and-swap on the manifest version: two connections
 * committing concurrently cannot both switch the manifest. The loser
 * re-reads the chain, re-checks idempotency/conflict/sequencing and rebinds
 * its batch to the actual head, so interleaved submits converge to a single
 * continuous chain and a batch is never reported sealed off a stale chain.
 * (putManifest is the unconditional replace used by recovery repairs;
 * switchManifest is the guarded switch used by the submit path.)
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

    // Another connection (a second review page on the same device) may
    // commit while this one is mid-protocol. Every decision below is made
    // against a freshly read manifest, and the manifest switch only lands
    // if that manifest is still current; otherwise the whole evaluation
    // restarts against the new head.
    for (let attempt = 1; attempt <= MAX_SUBMIT_ATTEMPTS; attempt += 1) {
      const manifest = await this.#loadManifest();

      // Already published under this batch id?
      if (Object.hasOwn(manifest.batches, batch.batchId)) {
        const existing = await this.#storage.getSegment(
          manifest.batches[batch.batchId]
        );
        if (existing && existing.contentHash === contentHash) {
          // Identical retransmission: hand back the original receipt, append nothing.
          return { status: "duplicate", receipt: existing.receipt };
        }
        // Different content under a published id: keep existing evidence, report.
        const conflict = {
          reason: "published-content-mismatch",
          batchId: batch.batchId,
          incomingContentHash: contentHash,
          existingContentHash: existing?.contentHash ?? null,
          existingReceipt: existing?.receipt ?? null,
          at: new Date().toISOString(),
        };
        await this.#recordConflict(conflict);
        return { status: "conflict", conflict };
      }

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
        if (intent.prevDigest === manifest.head) {
          // Same content, predecessor still current: resume the established
          // intent, do not start a new one.
          const outcome = await this.#fulfillIntent(intent, manifest, {
            persistIntent: false,
          });
          if (outcome.status !== "retry") return outcome;
          continue; // lost the manifest race: re-evaluate against the new head
        }
        // The recorded intent's predecessor is stale — another connection
        // committed since it was written. Re-establish the same batch as a
        // fresh intent bound to the actual head below; the stale prepared
        // segment stays behind as orphan evidence and is never linked.
      }

      // New batch: sequence numbers must extend the sealed record monotonically.
      const { lastSeqEnd } = await this.#headState(manifest);
      if (lastSeqEnd !== null && events[0].seq <= lastSeqEnd) {
        return {
          status: "rejected",
          error: {
            code: "sequence-regression",
            detail: `first seq ${events[0].seq} must be greater than the sealed last seq ${lastSeqEnd}`,
          },
        };
      }

      const newIntent = {
        batchId: batch.batchId,
        contentHash,
        events,
        prevDigest: manifest.head,
        expectedDigest: await segmentDigestOf(manifest.head, events),
        createdAt: intent?.createdAt ?? Date.now(),
      };
      const outcome = await this.#fulfillIntent(newIntent, manifest, {
        persistIntent: true,
      });
      if (outcome.status !== "retry") return outcome;
    }
    return {
      status: "blocked",
      error: {
        code: "manifest-contention",
        detail:
          "the active manifest kept switching under concurrent submits; the batch was not sealed",
      },
    };
  }

  // ---- persistence protocol --------------------------------------------

  async #fulfillIntent(intent, manifest, { persistIntent }) {
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

    // Stage 1: prepare intent.
    if (persistIntent) {
      await this.#storage.putPrepare(intent);
      await this.#maybeCrash("afterPrepare");
    }

    // Stage 2: immutable segment, content-addressed by its own digest.
    const segment = {
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
    await this.#storage.putSegment(segment);
    await this.#maybeCrash("afterSegment");

    // Stage 3: switch the active manifest — a compare-and-swap guarded by
    // the version this intent was built against. If another connection
    // switched the manifest first, nothing is published; the caller
    // re-evaluates the batch against the new head instead of reporting a
    // stale chain as sealed.
    const nextManifest = {
      ...manifest,
      segmentIds: [...manifest.segmentIds, segment.digest],
      batches: { ...manifest.batches, [segment.batchId]: segment.digest },
      head: segment.digest,
      version: manifest.version + 1,
    };
    const switched = await this.#storage.switchManifest(
      nextManifest,
      manifest.version
    );
    if (!switched) {
      return { status: "retry" };
    }
    await this.#maybeCrash("afterManifest");

    // Best-effort intent cleanup; a leftover fulfilled intent is swept by
    // recovery on the next open.
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
    const actions = [];
    const blocking = [];
    let manifest = await this.#loadManifest();

    // 1. Verify the manifest-pointed chain; keep the longest valid prefix.
    let prevDigest = GENESIS_DIGEST;
    let lastSeqEnd = null;
    const validIds = [];
    const validBatches = {};
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
        break; // first blocking evidence: everything from here on is excluded
      }
      validIds.push(digest);
      validBatches[segment.batchId] = digest;
      prevDigest = digest;
      lastSeqEnd = segment.seqEnd;
    }
    if (validIds.length !== manifest.segmentIds.length) {
      const dropped = manifest.segmentIds.slice(validIds.length);
      manifest = {
        ...manifest,
        segmentIds: validIds,
        batches: validBatches,
        head: prevDigest,
        version: manifest.version + 1,
      };
      await this.#storage.putManifest(manifest);
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
        await this.#storage.deletePrepare(intent.batchId);
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
        await this.#storage.deletePrepare(intent.batchId);
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
        await this.#storage.deletePrepare(intent.batchId);
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
      await this.#storage.putManifest(manifest);
      await this.#storage.deletePrepare(intent.batchId);
      lastSeqEnd = segment.seqEnd;
      actions.push({
        action: "segment-published-from-intent",
        batchId: intent.batchId,
        digest: segment.digest,
        seqStart: segment.seqStart,
        seqEnd: segment.seqEnd,
      });
    }

    const report = {
      at: new Date().toISOString(),
      actions,
      blocking,
      firstBlocking: blocking[0] ?? null,
      sealed: manifest.segmentIds.length,
      head: manifest.head,
    };
    await this.#storage.setMeta("lastRecovery", report);
    return report;
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
