// scripts/populate-prebreakout-fields.js
// This script reprocesses existing levels to populate pre-breakout V-shape fields

const path = require('path');
const eqhEqlEngine = require('../src/signals/dataProcessor/eqhEql');

/**
 * Populate pre-breakout fields for a specific symbol/granularity
 * Requires candles data to be available
 */
async function populatePreBreakoutFields(symbol, granularity, candles) {
  console.log(`\n${'='.repeat(80)}`);
  console.log(`Populating Pre-Breakout Fields: ${symbol} @ ${granularity}s`);
  console.log(`${'='.repeat(80)}\n`);
  
  if (!candles || candles.length === 0) {
    console.log('❌ No candles provided. Cannot populate fields.\n');
    return;
  }
  
  console.log(`Processing ${candles.length} candles...\n`);
  
  // Run detectAll to reprocess all levels with historical data
  // This will recalculate everything including pre-breakout fields
  const levels = await eqhEqlEngine.detectAll(symbol, granularity, candles);
  
  console.log(`\n✅ Reprocessed ${levels.length} levels\n`);
  
  // Check how many have pre-breakout data
  let populatedCount = 0;
  let nullCount = 0;
  
  for (const level of levels) {
    if (level.preBreakoutVDepth !== null) {
      populatedCount++;
    } else {
      nullCount++;
    }
  }
  
  console.log(`Results:`);
  console.log(`  Total levels: ${levels.length}`);
  console.log(`  With pre-breakout data: ${populatedCount} ✅`);
  console.log(`  Without pre-breakout data: ${nullCount} (no candles after second swing)`);
  
  if (populatedCount > 0) {
    console.log(`\n📊 Sample level with pre-breakout data:\n`);
    const sample = levels.find(l => l.preBreakoutVDepth !== null);
    if (sample) {
      console.log(`  Type: ${sample.type}`);
      console.log(`  Zone: ${sample.zoneBottom} - ${sample.zoneTop}`);
      console.log(`  Status: ${sample.status}`);
      console.log(`  \n  V-Shape (between swings):`);
      console.log(`    Depth: ${sample.vShapeDepth}`);
      console.log(`    Time: ${sample.vShapeFormattedTime}`);
      console.log(`  \n  Pre-Breakout V-Shape:`);
      console.log(`    Depth: ${sample.preBreakoutVDepth}`);
      console.log(`    Index: ${sample.preBreakoutVIndex}`);
      console.log(`    Time: ${sample.preBreakoutVFormattedTime}`);
    }
  }
  
  console.log(`\n${'='.repeat(80)}\n`);
  
  return levels;
}

/**
 * Example usage with your candle data source
 */
async function main() {
  console.log('Pre-Breakout Field Population Script\n');
  console.log('⚠️  This script requires candle data to be available.\n');
  console.log('You need to modify this script to load your candles from your data source.\n');
  console.log('Examples:');
  console.log('  - Load from database');
  console.log('  - Load from CSV file');
  console.log('  - Load from API');
  console.log('  - Use your existing candle loader\n');
  console.log('─'.repeat(80) + '\n');
  
  // EXAMPLE 1: Load from your existing candle source
  // const candleLoader = require('../src/data/candleLoader');
  // const candles = await candleLoader.load('BTCUSD', 3600);
  
  // EXAMPLE 2: Load from database
  // const db = require('../src/database');
  // const candles = await db.query('SELECT * FROM candles WHERE symbol = ? AND granularity = ?', ['BTCUSD', 3600]);
  
  // EXAMPLE 3: Hardcoded test data (replace with real data)
  const candles = []; // Your candles here
  
  if (candles.length === 0) {
    console.log('❌ No candles loaded. Please modify this script to load your candles.\n');
    console.log('See the examples in the main() function.\n');
    return;
  }
  
  // Process the data
  await populatePreBreakoutFields('R_25', 3600, candles);
  
  // You can add more symbols/granularities here
  // await populatePreBreakoutFields('ETHUSD', 3600, ethCandles);
}

// Export for use in other scripts
module.exports = {
  populatePreBreakoutFields,
};

// Run if called directly
if (require.main === module) {
  main().catch(console.error);
}