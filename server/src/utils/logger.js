// server/src/utils/logger.js
const config = require('../config');

const levels = {
  debug: 0,
  info:  1,
  warn:  2,
  error: 3,
};

const currentLevel = levels[config.LOG_LEVEL] ?? levels.info;

class Logger {
  constructor(prefix = '') {
    this.prefix = prefix ? `[${prefix}] ` : '';
  }

  debug(...args) {
    if (currentLevel <= levels.debug) console.debug(this.prefix, ...args);
  }

  info(...args) {
    if (currentLevel <= levels.info) console.info(this.prefix, ...args);
  }

  warn(...args) {
    if (currentLevel <= levels.warn) console.warn(this.prefix, ...args);
  }

  error(...args) {
    if (currentLevel <= levels.error) console.error(this.prefix, ...args);
  }
}

module.exports = Logger;