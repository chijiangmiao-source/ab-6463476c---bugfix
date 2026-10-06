// IndexedDB storage adapter for the sealing engine. Each write is its own
// committed transaction, so the persistence protocol (prepare intent ->
// immutable segment -> active manifest) survives a tab crash exactly the
// way the drills simulate it.

const DB_NAME = "glider-seal";
const DB_VERSION = 1;

function requestToPromise(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

export class IdbStorage {
  #db;

  constructor(db) {
    this.#db = db;
  }

  static async open() {
    const db = await new Promise((resolve, reject) => {
      const request = indexedDB.open(DB_NAME, DB_VERSION);
      request.onupgradeneeded = () => {
        const upgrading = request.result;
        upgrading.createObjectStore("prepares", { keyPath: "batchId" });
        upgrading.createObjectStore("segments", { keyPath: "digest" });
        upgrading.createObjectStore("manifest", { keyPath: "id" });
        upgrading.createObjectStore("meta", { keyPath: "key" });
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    return new IdbStorage(db);
  }

  close() {
    this.#db.close();
  }

  #read(storeName, fn) {
    const tx = this.#db.transaction(storeName, "readonly");
    return requestToPromise(fn(tx.objectStore(storeName)));
  }

  // Writes resolve on transaction completion, i.e. when data is durable.
  #write(storeName, fn) {
    return new Promise((resolve, reject) => {
      const tx = this.#db.transaction(storeName, "readwrite");
      fn(tx.objectStore(storeName));
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error ?? new Error("transaction aborted"));
    });
  }

  async getPrepare(batchId) {
    return (await this.#read("prepares", (s) => s.get(batchId))) ?? null;
  }

  async putPrepare(record) {
    await this.#write("prepares", (s) => s.put(record));
  }

  async deletePrepare(batchId) {
    await this.#write("prepares", (s) => s.delete(batchId));
  }

  async listPrepares() {
    return await this.#read("prepares", (s) => s.getAll());
  }

  async getSegment(digest) {
    return (await this.#read("segments", (s) => s.get(digest))) ?? null;
  }

  async putSegment(record) {
    await this.#write("segments", (s) => s.put(record));
  }

  async getManifest() {
    return (await this.#read("manifest", (s) => s.get("active"))) ?? null;
  }

  async putManifest(record) {
    const current = await this.getManifest();
    const currentIds = Array.isArray(current?.segmentIds)
      ? current.segmentIds
      : [];
    const incomingIds = Array.isArray(record.segmentIds)
      ? record.segmentIds
      : [];
    const segmentIds = [...new Set([...currentIds, ...incomingIds])];
    const currentBatches = current?.batches ?? {};
    const incomingBatches = record.batches ?? {};
    const batches = { ...currentBatches, ...incomingBatches };
    const version = Math.max(current?.version ?? 0, record.version ?? 0);
    const manifest = { ...record, segmentIds, batches, version, id: "active" };
    await this.#write("manifest", (s) => s.put(manifest));
  }

  async getMeta(key) {
    const row = await this.#read("meta", (s) => s.get(key));
    return row ? row.value : null;
  }

  async setMeta(key, value) {
    await this.#write("meta", (s) => s.put({ key, value }));
  }
}
