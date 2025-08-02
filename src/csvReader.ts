import * as csv from "fast-csv";
import * as fs from "fs";

export function readCsv<T>(filePath: string): Promise<T[]> {
    return new Promise((resolve, reject) => {
        const results: T[] = [];
        
        if (!fs.existsSync(filePath)) {
            reject(new Error(`File not found: ${filePath}`));
            return;
        }

        fs.createReadStream(filePath)
            .pipe(csv.parse({ headers: true,ignoreEmpty: true }))
            .on('data', (data: T) => {
                results.push(data);
            })
            .on('end', () => {
                console.log(`Finished reading ${filePath}. Total rows: ${results.length}`);
                resolve(results);
            })
            .on('error', (error: Error) => {
                console.error(`Error reading ${filePath}:`, error.message);
                reject(error);
            });
    });
}

