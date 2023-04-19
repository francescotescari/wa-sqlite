const DURATION_MILLIS = 10 * 1000;

const DATABASE_CONFIGS = new Map([
  {
    label: 'IDBMinimalVFS',
    isAsync: true,
    vfsModule: '../src/examples/IDBMinimalVFS.js',
    vfsClass: 'IDBMinimalVFS',
    vfsArgs: ['demo-IDBMinimalVFS']
  },
  {
    label: 'IDBBatchAtomicVFS',
    isAsync: true,
    vfsModule: '../src/examples/IDBBatchAtomicVFS.js',
    vfsClass: 'IDBBatchAtomicVFS',
    vfsArgs: ['demo-IDBBatchAtomicVFS']
  },
  {
    label: 'OriginPrivateFileSystemVFS',
    isAsync: true,
    vfsModule: '../src/examples/OriginPrivateFileSystemVFS.js',
    vfsClass: 'OriginPrivateFileSystemVFS',
    vfsArgs: []
  }
].map(value => [value.label, value]));

const DEFAULT_VFS = 'IDBBatchAtomicVFS';

const DATE_OPTIONS = {
  hour12: false,
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  fractionalSecondDigits: 3
};

class ContentionDemo extends EventTarget {
  #tabId = Math.random().toString(36).replace('0.', '');
  #sharedWorker = new SharedWorker('./contention-sharedworker.js');

  #dbProxy;

  constructor() {
    super();

    const params = new URLSearchParams(window.location.search);
    this.#prepare(params.get('vfs') || DEFAULT_VFS, params.has('clear'));

    this.#sharedWorker.port.start();

    new BroadcastChannel('clients').addEventListener('message', ({data}) => {
      this.dispatchEvent(new CustomEvent('clients', { detail: data }));
    });
  
    document.getElementById('newtab').addEventListener('click', () => {
      window.open(window.location.href, '_blank');
    });
  }

  async requestStart() {
    await this.#dbProxy`
      CREATE TABLE IF NOT EXISTS kv (key PRIMARY KEY, value);
      REPLACE INTO kv VALUES ('counter', 0);

      CREATE TABLE IF NOT EXISTS log (time, tabId, count);
      DELETE FROM log;
    `;

    this.#sharedWorker.port.postMessage({
      type: 'go',
      duration: DURATION_MILLIS
    });
  }

  async #prepare(vfs, clear) {
    try {
      const vfsConfig = DATABASE_CONFIGS.get(vfs);
      if (!vfsConfig) throw new Error(`Bad VFS: ${vfs}`);

      if (clear) {
        this.#log('clearing storage');
        localStorage.clear();
        const worker = new Worker('./clean-worker.js', { type: 'module' });
        await new Promise(resolve => {
          worker.addEventListener('message', resolve);
        });
        worker.terminate();
      }

      // Instantiate the database Worker.
      const Comlink = await import(location.hostname.endsWith('localhost') ?
        '/.yarn/unplugged/comlink-npm-4.4.1-b05bb2527d/node_modules/comlink/dist/esm/comlink.min.js' :
        'https://unpkg.com/comlink/dist/esm/comlink.mjs');

      const worker = new Worker('./demo-worker.js', { type: 'module' });
      await new Promise(resolve => {
        worker.addEventListener('message', resolve, { once: true });
      });
      const workerProxy = Comlink.wrap(worker);
      this.#dbProxy = await workerProxy(vfsConfig);

      navigator.locks.request(this.#tabId, () => new Promise(() => {
        // Register with the SharedWorker.
        this.#sharedWorker.port.postMessage({
          type: 'register',
          name: this.#tabId
        });

        new BroadcastChannel('go').addEventListener('message', ({data}) => {
          this.#go(data);
        });
        this.dispatchEvent(new CustomEvent('ready'));

        // This Promise never resolves so we keep the lock until exit.
      }));
    } catch (e) {
      this.#logError(e);
      throw e;
    }
  }

  async #go(endTime) {
    try {
      this.dispatchEvent(new CustomEvent('go', { detail: endTime }));
      while (Date.now() < endTime) {
        await this.#dbProxy`
          BEGIN IMMEDIATE;

          UPDATE kv SET value = value + 1 WHERE key = 'counter';
          INSERT INTO log VALUES
            (${Date.now()}, '${this.#tabId}', (SELECT value FROM kv WHERE key = 'counter'));

          COMMIT;
        `
      }

      const results = await this.#dbProxy`
        DELETE FROM log WHERE time > ${endTime};

        SELECT COUNT(*) FROM log GROUP BY tabId;
      `;
      const counts = results[0].rows.flat();
      const sum = counts.reduce((sum, value) => sum + value);
      this.#log(`transactions by tab ${JSON.stringify(counts)} => ${sum}`);
      this.dispatchEvent(new CustomEvent('ready'));
      console.log(results);
    } catch (e) {
      this.#logError(e);
      throw e;
    }
  }

  #log(s) {
    // @ts-ignore
    const timestamp = new Date().toLocaleTimeString(undefined, DATE_OPTIONS);
    const value = `${timestamp} ${s}`;
    this.dispatchEvent(new CustomEvent('log', { detail: value }));
  }

  #logError(e) {
    const s = e.stack.includes(e.message) ? e.stack : `${e.message}\n${e.stack}`;
    this.#log(s);
  }
}

const demo = new ContentionDemo();

demo.addEventListener('clients', function(/** @type {CustomEvent} */ event) {
  document.getElementById('clientCount').textContent = String(event.detail);
});

demo.addEventListener('ready', function(/** @type {CustomEvent} */ event) {
    // @ts-ignore
    document.getElementById('start').disabled = false;
});

demo.addEventListener('go', function countDown(/** @type {CustomEvent} */ event) {
  // @ts-ignore
  document.getElementById('start').disabled = true;
});

demo.addEventListener('go', function countDown(/** @type {CustomEvent} */ event) {
  const endTime = event.detail;
  const clock = document.getElementById('clock');

  const now = Date.now();
  if (now < endTime) {
    const value = Math.round((endTime - now) / 1000);
    clock.textContent = value.toString();
    setTimeout(() => countDown(event), 1000);
  } else {
    clock.textContent = '';
  }
});

demo.addEventListener('log', function countDown(/** @type {CustomEvent} */ event) {
  const output = document.getElementById('output');
  const pre = document.createElement('pre');
  output.appendChild(pre);

  pre.textContent = event.detail;
});

document.getElementById('start').addEventListener('click', function() {
  demo.requestStart();
});