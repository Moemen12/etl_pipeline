import * as path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import type { CaregiverCsvRow, CarelogCsvRow } from './types.js';
import { readCsv } from './csvReader.js';
import { transformCaregivers, transformCarelogs } from './dataTransformer.js';
import { insertCaregivers, insertCarelogs, initDatabase, closeDatabasePool } from './database.js';

// Get __dirname equivalent for ES modules Cause __dirname doesn't work here
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function runEtl() {
    const totalStartTime = performance.now();
    console.log('🚀 Starting ETL process...');
    
    try {
        // 1. Extract - Caregivers
        const caregiverExtractStart = performance.now();
        const caregiversCsvPath = path.join(__dirname, '..', 'data', 'caregivers.csv');
        const caregiverCsvRows = await readCsv<CaregiverCsvRow>(caregiversCsvPath);
        const caregiverExtractTime = performance.now() - caregiverExtractStart;
        
        console.log(`📁 Extract Caregivers: ${caregiverCsvRows.length.toLocaleString()} rows in ${(caregiverExtractTime / 1000).toFixed(2)}s`);

        // 1. Extract - Carelogs
        const carelogExtractStart = performance.now();
        const carelogsCsvPath = path.join(__dirname, '..', 'data', 'carelogs.csv');
        const carelogCsvRows = await readCsv<CarelogCsvRow>(carelogsCsvPath);
        const carelogExtractTime = performance.now() - carelogExtractStart;
        
        console.log(`📁 Extract Carelogs: ${carelogCsvRows.length.toLocaleString()} rows in ${(carelogExtractTime / 1000).toFixed(2)}s`);

        // 2. Transform - Caregivers
        const caregiverTransformStart = performance.now();
        const transformedCaregivers = transformCaregivers(caregiverCsvRows);
        const caregiverTransformTime = performance.now() - caregiverTransformStart;
        
        console.log(`🔄 Transform Caregivers: ${transformedCaregivers.length.toLocaleString()} records in ${(caregiverTransformTime / 1000).toFixed(2)}s`);

        // 2. Transform - Carelogs
        const carelogTransformStart = performance.now();
        const transformedCarelogs = transformCarelogs(carelogCsvRows);
        const carelogTransformTime = performance.now() - carelogTransformStart;
        
        console.log(`🔄 Transform Carelogs: ${transformedCarelogs.length.toLocaleString()} records in ${(carelogTransformTime / 1000).toFixed(2)}s`);

        // 3. Load - Database Init
        const dbInitStart = performance.now();
        await initDatabase();
        const dbInitTime = performance.now() - dbInitStart;
        
        console.log(`💾 Database Init: ${(dbInitTime / 1000).toFixed(2)}s`);

        // 3. Load - Insert Caregivers
        const caregiverInsertStart = performance.now();
        await insertCaregivers(transformedCaregivers);
        const caregiverInsertTime = performance.now() - caregiverInsertStart;
        
        console.log(`💾 Insert Caregivers: ${transformedCaregivers.length.toLocaleString()} records in ${(caregiverInsertTime / 1000).toFixed(2)}s`);

        // 3. Load - Insert Carelogs
        const carelogInsertStart = performance.now();
        await insertCarelogs(transformedCarelogs);
        const carelogInsertTime = performance.now() - carelogInsertStart;
        
        console.log(`💾 Insert Carelogs: ${transformedCarelogs.length.toLocaleString()} records in ${(carelogInsertTime / 1000).toFixed(2)}s`);
        
        // Summary
        const totalTime = performance.now() - totalStartTime;
        const totalRecords = transformedCaregivers.length + transformedCarelogs.length;
        
        console.log(`🎉 ETL Completed: ${totalRecords.toLocaleString()} total records in ${(totalTime / 1000).toFixed(2)}s`);
        console.log(`📊 Performance: ${Math.round(totalRecords / (totalTime / 1000)).toLocaleString()} records/second`);
        
    } catch (error) {
        console.error('❌ ETL failed:', error);
        process.exit(1);
    } finally {
        await closeDatabasePool();
    }
}

runEtl();