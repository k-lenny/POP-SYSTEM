// scripts/redetect-all.js
// Run with: node scripts/redetect-all.js (while server is running)
const fetch = require('node-fetch');

// Adjust these lists to match the symbols and granularities you track
const symbols = [
  'R_10',      // Volatility 10
  '1HZ10V',    // Volatility 10s
  'R_25',      // Volatility 25
  '1HZ25V',    // Volatility 25s
  'R_50',      // Volatility 50
  '1HZ50V',    // Volatility 50s
  'R_75',      // Volatility 75
  '1HZ75V',    // Volatility 75s
  'R_100',     // Volatility 100
  '1HZ100V',   // Volatility 100s
  '1HZ150V',   // Volatility 150s
  '1HZ250V'    // Volatility 250s
];
// All timeframes from signalEngine.timeframes (in seconds)
const granularities = [
  60,     // 1 min
  120,    // 2 min
  180,    // 3 min
  300,    // 5 min
  600,    // 10 min
  900,    // 15 min
  1800,   // 30 min
  3600,   // 1 hour
  7200,   // 2 hours
  14400,  // 4 hours
  28800,  // 8 hours
  86400   // 24 hours
];

const BASE_URL = 'http://localhost:4000'; // change if your server runs elsewhere

async function redetectAll() {
  console.log('Starting redetection for all symbol/granularity combinations...\n');
  for (const symbol of symbols) {
    for (const gran of granularities) {
      const url = `${BASE_URL}/eqheql/redetect/${symbol}/${gran}`;
      console.log(`POST ${url}`);
      try {
        const res = await fetch(url, { method: 'POST' });
        const data = await res.json();
        if (res.ok) {
          console.log(`  ✅ ${data.message} – ${data.count} levels regenerated`);
        } else {
          console.log(`  ❌ Error: ${data.message || res.statusText}`);
        }
      } catch (err) {
        console.log(`  ❌ Failed: ${err.message}`);
      }
      // small delay to avoid overwhelming the server
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }
  console.log('\nRedetection complete.');
}

redetectAll();