// Copyright 2023 Roy T. Hashimoto. All Rights Reserved.

import * as SQLite from '../src/sqlite-api.js';

import { createTag } from "../src/examples/tag.js";

// For a typical application, the Emscripten module would be imported
// statically, but we want to be able to select between the Asyncify
// and non-Asyncify builds so dynamic import is done later.
const WA_SQLITE = '../dist/wa-sqlite.mjs';
const WA_SQLITE_ASYNC = '../dist/wa-sqlite-async.mjs';

/**
 * @typedef Config
 * @property {boolean} isAsync use WebAssembly build with/without Asyncify
 * @property {string} [dbName] name of the SQLite database
 * @property {string} [vfsModule] path of the VFS module
 * @property {string} [vfsClass] name of the VFS class
 * @property {Array<*>} [vfsArgs] VFS constructor arguments
 */

(async function() {
  const Comlink = await import(location.hostname.endsWith('localhost') ?
    '/.yarn/unplugged/comlink-npm-4.4.1-b05bb2527d/node_modules/comlink/dist/esm/comlink.min.js' :
    'https://unpkg.com/comlink/dist/esm/comlink.mjs');

  const jobs = [];
  const mutex = new Mutex();
  async function init(sqlite3, config, index) {
    // Open the database;
    const name = (config.dbName ?? 'demo')+index+'.sqlite';
    console.log(`Opening database ${name}`);
    const db = await sqlite3.open_v2(name);
    return createTag(sqlite3, db);
  }

  /**
   * @param {Config} config
   * @returns {Promise<Function>}
   */
  async function open(config) {
    // Instantiate the SQLite API, choosing between Asyncify and non-Asyncify.
    const { default: moduleFactory } = await import(config.isAsync ? WA_SQLITE_ASYNC : WA_SQLITE);
    const concurrentJobs = config.concurrency ?? 1;
    const useMutex = config.useMutex ? mutex.run.bind(mutex) : (fn => fn());
    const module = await moduleFactory();
    const sqlite3 = SQLite.Factory(module);

    if (config.vfsModule) {
      // Create the VFS and register it as the default file system.
      const namespace = await import(config.vfsModule);
      const vfs = new namespace[config.vfsClass](...config.vfsArgs ?? []);
      await vfs.isReady;
      sqlite3.vfs_register(vfs, true);
    }

    // Concurrently open the databases.
    for (let i = 0; i < concurrentJobs; i++) {
      const job = useMutex(() => init(sqlite3, config, i));
      jobs.push(job);
    }
    await Promise.all(jobs);

    // Create the query interface.
    const tag = async function() {
      const results = [];
      for (let i = 0; i < concurrentJobs; i++) {
        const result = jobs[i]
          .then(sql =>  useMutex(() => sql(...arguments)))
          .catch(err => console.error(`Failed to run job ${i}: ${err.message}`));
        // Concurrency only between jobs, not within a job.
        jobs[i] = jobs[i].then(sql => result.then(() => sql));
        results.push(result);
      }
      return (await Promise.all(results))[0];
    }
    return Comlink.proxy(tag);
  }

  postMessage(null);
  Comlink.expose(open);
})();


class Mutex {
  promise = Promise.resolve();
  run(fn) {
    const result = this.promise.then(fn);
    this.promise = result.catch(() => {});
    return result;
  }
}
