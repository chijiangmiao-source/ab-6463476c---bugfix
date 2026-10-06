// In-memory storage adapter. The backing object survives the engine that
// uses it, so a drill can "crash" one engine and "reopen" a fresh engine
// over the same persisted state — mirroring how the browser page closes
// and reopens its IndexedDB connection.

export function createMemoryBacking() {
  return {
    prepares: new Map(), // batchId -> intent
    segments: new Map(), // digest -> immutable segment
    manifest: null, // single active manifest
    meta: new Map(), // key -> value (recovery report, conflict log)
  };
}

const clone = (value) =>
  value === undefined || value === null ? value : structuredClone(value);

export class MemoryStorage {
  constructor(backing = createMemoryBacking()) {
    this.backing = backing;
  }

  async getPrepare(batchId) {
    return clone(this.backing.prepares.get(batchId) ?? null);
  }

  async putPrepare(record) {
    this.backing.prepares.set(record.batchId, clone(record));
  }

  async deletePrepare(batchId) {
    this.backing.prepares.delete(batchId);
  }

  async listPrepares() {
    return [...this.backing.prepares.values()].map(clone);
  }

  async getSegment(digest) {
    return clone(this.backing.segments.get(digest) ?? null);
  }

  async putSegment(record) {
    this.backing.segments.set(record.digest, clone(record));
  }

  async getManifest() {
    return clone(this.backing.manifest);
  }

  // Atomically switch the active manifest, but only if the stored manifest
  // is still at expectedVersion (compare-and-swap). Several adapters can
  // share one backing — two pages on one device — so a stale snapshot must
  // never clobber a manifest a concurrent connection already switched.
  async switchManifest(expectedVersion, record) {
    const currentVersion = this.backing.manifest?.version ?? 0;
    if (currentVersion !== expectedVersion) return false;
    this.backing.manifest = clone(record);
    return true;
  }

  async getMeta(key) {
    return clone(this.backing.meta.get(key) ?? null);
  }

  async setMeta(key, value) {
    this.backing.meta.set(key, clone(value));
  }
}
