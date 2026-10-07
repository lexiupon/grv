'use strict';
function installReadOnlyKeychain(implementation, key) {
  if (!implementation || typeof implementation.getPassword !== 'function'
      || typeof implementation.setPassword !== 'function') throw new Error('private keychain bridge refused');
  Object.defineProperties(implementation, {
    getPassword: { value: async (opts, callback) => {
      if (!opts || Object.keys(opts).sort().join(',') !== 'account,service'
          || opts.account !== 'local' || opts.service !== 'sfdx') {
        callback(new Error('private keychain lookup refused')); return;
      }
      callback(null, key);
    }, writable: false, configurable: false },
    setPassword: { value: async (_opts, callback) => callback(new Error('private keychain mutation refused')),
      writable: false, configurable: false }
  });
}
