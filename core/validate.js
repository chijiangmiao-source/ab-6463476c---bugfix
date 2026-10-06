// Batch validation rules. A batch carries a stable batch id, up to 24
// events with strictly increasing sequence numbers, and text payloads.

export const MAX_EVENTS_PER_BATCH = 24;
export const MAX_BATCH_ID_LENGTH = 64;
export const MAX_PAYLOAD_LENGTH = 4096;
export const BATCH_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;

function fail(code, detail) {
  return { ok: false, code, detail };
}

export function validateBatch(batch) {
  if (batch === null || typeof batch !== "object" || Array.isArray(batch)) {
    return fail("batch-not-object", "batch must be an object");
  }
  const { batchId, events } = batch;

  if (typeof batchId !== "string" || !BATCH_ID_PATTERN.test(batchId)) {
    return fail(
      "invalid-batch-id",
      `batchId must match ${BATCH_ID_PATTERN} (1-${MAX_BATCH_ID_LENGTH} chars)`
    );
  }
  if (!Array.isArray(events) || events.length === 0) {
    return fail("events-empty", "events must be a non-empty array");
  }
  if (events.length > MAX_EVENTS_PER_BATCH) {
    return fail(
      "events-too-many",
      `events must contain at most ${MAX_EVENTS_PER_BATCH} entries, got ${events.length}`
    );
  }

  let prevSeq = null;
  for (let i = 0; i < events.length; i += 1) {
    const event = events[i];
    if (event === null || typeof event !== "object" || Array.isArray(event)) {
      return fail("event-not-object", `events[${i}] must be an object`);
    }
    if (!Number.isSafeInteger(event.seq) || event.seq < 0) {
      return fail(
        "invalid-seq",
        `events[${i}].seq must be a non-negative safe integer`
      );
    }
    if (typeof event.payload !== "string") {
      return fail("invalid-payload", `events[${i}].payload must be a string`);
    }
    if (event.payload.length > MAX_PAYLOAD_LENGTH) {
      return fail(
        "payload-too-long",
        `events[${i}].payload exceeds ${MAX_PAYLOAD_LENGTH} chars`
      );
    }
    if (prevSeq !== null && event.seq <= prevSeq) {
      return fail(
        "seq-not-increasing",
        `events[${i}].seq (${event.seq}) must be strictly greater than the previous seq (${prevSeq})`
      );
    }
    prevSeq = event.seq;
  }
  return { ok: true };
}
