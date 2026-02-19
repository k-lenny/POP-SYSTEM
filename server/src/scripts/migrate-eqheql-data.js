// scripts/migrate-eqheql-data.js
// This script migrates existing EQH/EQL data files to include new pre-breakout fields
// AND backfills them from historical candles (requires server to be running).

const fs = require('fs').promises;
const path = require('path');
const signalEngine = require('../signals/signalEngine'); // adjust path as needed

// Helper to compute pre‑breakout extreme for a level
async function backfillLevel(symbol, granularity, level, candles, candleIndexMap) {
  // Only process if preBreakoutVDepth is null (i.e., not already set)
  if (level.preBreakoutVDepth !== null) return false;

  const secondSwingIdx = level.secondSwingIndex;
  const brokenIdx = level.brokenIndex; // may be null if still active

  const startPos = candleIndexMap.get(secondSwingIdx);
  if (startPos === undefined) return false;
  const start = startPos + 1;

  const endPos = brokenIdx !== null ? candleIndexMap.get(brokenIdx) : candles.length - 1;
  if (endPos === undefined) return false;
  if (start > endPos) return false;

  let extremeValue = level.type === 'EQH' ? Infinity : -Infinity;
  let extremeCandle = null;

  for (let i = start; i <= endPos; i++) {
    const c = candles[i];
    if (level.type === 'EQH' && c.low < extremeValue) {
      extremeValue = c.low;
      extremeCandle = c;
    } else if (level.type === 'EQL' && c.high > extremeValue) {
      extremeValue = c.high;
      extremeCandle = c;
    }
  }

  if (extremeCandle) {
    level.preBreakoutVDepth = extremeValue;
    level.preBreakoutVIndex = extremeCandle.index;
    level.preBreakoutVTime = extremeCandle.time;
    level.preBreakoutVFormattedTime = extremeCandle.formattedTime;
    return true;
  }
  return false;
}

/**
 * Migrate a single JSON file
 */
async function migrateFile(filePath, dryRun = false) {
  const fileName = path.basename(filePath);
  const match = fileName.match(/^(.+)_(\d+)\.json$/);
  if (!match) {
    console.log(`⚠️  Skipping ${fileName} – invalid filename format`);
    return { migrated: 0, total: 0 };
  }
  const symbol = match[1];
  const granularity = parseInt(match[2]);

  // Load candles from signalEngine
  const candles = signalEngine.getCandles(symbol, granularity, true); // with index
  if (!candles || candles.length === 0) {
    console.log(`⚠️  No candles for ${symbol} @ ${granularity}, skipping ${fileName} (server must be running and data loaded)`);
    return { migrated: 0, total: 0 };
  }
  const candleIndexMap = new Map(candles.map((c, i) => [c.index, i]));

  const content = await fs.readFile(filePath, 'utf8');
  const data = JSON.parse(content);

  if (!data.store || !Array.isArray(data.store)) {
    console.log(`⚠️  Skipping ${fileName} - no store array found`);
    return { migrated: 0, total: 0 };
  }

  let migratedCount = 0;
  let backfilledCount = 0;

  for (const level of data.store) {
    // First, ensure all fields exist (add null if missing)
    let changed = false;
    if (!level.hasOwnProperty('preBreakoutVIndex')) {
      level.preBreakoutVIndex = null;
      changed = true;
    }
    if (!level.hasOwnProperty('preBreakoutVTime')) {
      level.preBreakoutVTime = null;
      changed = true;
    }
    if (!level.hasOwnProperty('preBreakoutVDepth')) {
      level.preBreakoutVDepth = null;
      changed = true;
    }
    if (!level.hasOwnProperty('preBreakoutVFormattedTime')) {
      level.preBreakoutVFormattedTime = null;
      changed = true;
    }
    if (changed) migratedCount++;

    // Now attempt to backfill if still null
    if (await backfillLevel(symbol, granularity, level, candles, candleIndexMap)) {
      backfilledCount++;
    }
  }

  if (migratedCount > 0 || backfilledCount > 0) {
    if (!dryRun) {
      // Create backup
      const backupPath = filePath + '.backup';
      await fs.copyFile(filePath, backupPath);
      // Write updated data
      await fs.writeFile(filePath, JSON.stringify(data, null, 2));
      console.log(`✅ ${fileName}: Migrated ${migratedCount}, backfilled ${backfilledCount} / ${data.store.length} levels (backup created)`);
    } else {
      console.log(`🔍 ${fileName}: Would migrate ${migratedCount}, backfill ${backfilledCount} / ${data.store.length} levels`);
    }
  } else {
    console.log(`✓  ${fileName}: Already up to date (${data.store.length} levels)`);
  }

  return { migrated: migratedCount, backfilled: backfilledCount, total: data.store.length };
}

/**
 * Migrate all JSON files in a directory
 */
async function migrateDirectory(dataDir, dryRun = false) {
  console.log(`\n${'='.repeat(80)}`);
  console.log(`EQH/EQL Data Migration & Backfill Script`);
  console.log(`${'='.repeat(80)}\n`);
  console.log(`Directory: ${dataDir}`);
  console.log(`Mode: ${dryRun ? 'DRY RUN (no changes will be made)' : 'LIVE (files will be updated)'}`);
  console.log(`\n⚠️  IMPORTANT: This script requires the server to be running and all historical data loaded.`);
  console.log(`\n${'─'.repeat(80)}\n`);

  try {
    const files = await fs.readdir(dataDir);
    const jsonFiles = files.filter(f => f.endsWith('.json') && !f.endsWith('.backup'));

    if (jsonFiles.length === 0) {
      console.log(`⚠️  No JSON files found in ${dataDir}\n`);
      return;
    }

    console.log(`Found ${jsonFiles.length} file(s) to check\n`);

    let totalMigrated = 0;
    let totalBackfilled = 0;
    let totalLevels = 0;
    let filesUpdated = 0;

    for (const file of jsonFiles) {
      const filePath = path.join(dataDir, file);
      const result = await migrateFile(filePath, dryRun);

      if (result.migrated > 0 || result.backfilled > 0) {
        filesUpdated++;
      }

      totalMigrated += result.migrated || 0;
      totalBackfilled += result.backfilled || 0;
      totalLevels += result.total;
    }

    console.log(`\n${'─'.repeat(80)}\n`);
    console.log(`Summary:`);
    console.log(`  Files processed: ${jsonFiles.length}`);
    console.log(`  Files updated: ${filesUpdated}`);
    console.log(`  Levels with added null fields: ${totalMigrated}`);
    console.log(`  Levels backfilled with real values: ${totalBackfilled}/${totalLevels}`);

    if (dryRun && (totalMigrated > 0 || totalBackfilled > 0)) {
      console.log(`\n💡 Run with --live flag to apply changes`);
    } else if (!dryRun && filesUpdated > 0) {
      console.log(`\n✅ Migration complete! Backup files created with .backup extension`);
    }

    console.log(`\n${'='.repeat(80)}\n`);

  } catch (err) {
    console.error(`❌ Error reading directory: ${err.message}`);
  }
}

// Run if called directly
if (require.main === module) {
  const args = process.argv.slice(2);
  const dryRun = !args.includes('--live');

  const defaultDataDir = path.join(__dirname, '../data/eqhEql');
  const dataDir = args.find(arg => !arg.startsWith('--')) || defaultDataDir;

  console.log(`\n💡 Default data directory: ${defaultDataDir}`);
  console.log(`   (You can override with: node migrate-eqheql-data.js /custom/path --live)\n`);

  migrateDirectory(dataDir, dryRun).catch(console.error);
}

module.exports = {
  migrateLevel: (level) => { /* kept for compatibility, but not used */ },
  migrateFile,
  migrateDirectory,
};