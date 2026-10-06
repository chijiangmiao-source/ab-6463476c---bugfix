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

  // Unconditional replace — used by recovery repairs (truncate / publish),
  // never by the submit path.
  async putManifest(record) {
    await this.#write("manifest", (s) => s.put(record));
  }

  // Compare-and-swap manifest switch for the submit path: the new manifest
  // is stored only if the currently stored version still equals
  // `expectedVersion`. The read and the write share one transaction, and
  // IndexedDB serializes readwrite transactions on the same object store,
  // so two tabs on this device cannot both win the switch — the loser sees
  // the new version and re-evaluates against the new head.
  async switchManifest(record, expectedVersion) {
    return await new Promise((resolve, reject) => {
      const tx = this.#db.transaction("manifest", "readwrite");
      const store = tx.objectStore("manifest");
      const read = store.get("active");
      let switched = false;
      read.onsuccess = () => {
        const current = read.result ?? null;
        if ((current?.version ?? 0) === expectedVersion) {
          store.put(record);
          switched = true;
        }
      };
      tx.oncomplete = () => resolve(switched);
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error ?? new Error("transaction aborted"));
    });
  }

  async getMeta(key) {
    const row = await this.#read("meta", (s) => s.get(key));
    return row ? row.value : null;
  }

  async setMeta(key, value) {
    await this.#write("meta", (s) => s.put({ key, value }));
  }
}
