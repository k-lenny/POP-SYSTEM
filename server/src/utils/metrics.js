// server/src/utils/metrics.js

class Metrics {
  constructor() {
    this.counters = {};
    this.timers = {};
  }

  increment(counter, value = 1) {
    if (!this.counters[counter]) this.counters[counter] = 0;
    this.counters[counter] += value;
  }

  decrement(counter, value = 1) {
    if (!this.counters[counter]) this.counters[counter] = 0;
    this.counters[counter] -= value;
  }

  set(counter, value) {
    this.counters[counter] = value;
  }

  get(counter) {
    return this.counters[counter] || 0;
  }

  getAll() {
    return { ...this.counters };
  }

  // Simple timer
  startTimer(name) {
    this.timers[name] = Date.now();
  }

  endTimer(name) {
    const start = this.timers[name];
    if (start) {
      const duration = Date.now() - start;
      this.increment(`${name}_total`, duration);
      this.increment(`${name}_count`, 1);
      delete this.timers[name];
      return duration;
    }
    return null;
  }

  reset() {
    this.counters = {};
    this.timers = {};
  }
}

module.exports = new Metrics();