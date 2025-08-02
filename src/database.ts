
import { Pool } from "pg";
import dotenv from "dotenv";
import type { Caregiver, Carelog } from "./types.js";

dotenv.config();


const pool = new Pool({
    user: process.env.DATABASE_USER,
    host: process.env.DATABASE_HOST,
    database: process.env.DATABASE_NAME,
    password: process.env.DATABASE_PASSWORD,
    port: Number(process.env.DATABASE_PORT),
    max: 20,
});


export async function initDatabase(): Promise<void> {
    try {
        const client = await pool.connect();
        // Create caregivers table
        await client.query(`
            CREATE TABLE IF NOT EXISTS caregivers (
                franchisor_id VARCHAR(255) NOT NULL,
                agency_id VARCHAR(255) NOT NULL,
                subdomain VARCHAR(255),
                profile_id VARCHAR(255) PRIMARY KEY,
                caregiver_id VARCHAR(255) NOT NULL UNIQUE,
                external_id VARCHAR(255),
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                email VARCHAR(255),
                phone_number VARCHAR(255),
                gender VARCHAR(50),
                applicant BOOLEAN,
                birthday_date DATE,
                onboarding_date TIMESTAMP,
                location_name VARCHAR(255),
                locations_id INT,
                applicant_status VARCHAR(255),
                status VARCHAR(50)
            );
        `);

        await client.query(`
            CREATE TABLE IF NOT EXISTS carelogs (
                franchisor_id VARCHAR(255) NOT NULL,
                agency_id VARCHAR(255) NOT NULL,
                carelog_id VARCHAR(255) PRIMARY KEY,
                caregiver_id VARCHAR(255) NOT NULL,
                parent_id VARCHAR(255),
                start_datetime TIMESTAMP NOT NULL,
                end_datetime TIMESTAMP NOT NULL,
                clock_in_actual_datetime TIMESTAMP,
                clock_out_actual_datetime TIMESTAMP,
                clock_in_method INT,
                clock_out_method INT,
                status INT,
                split BOOLEAN,
                documentation TEXT,
                general_comment_char_count INT,
                FOREIGN KEY (caregiver_id) REFERENCES caregivers(caregiver_id)
            );
        `);

        client.release();

    } catch (error) {
        console.error('Error initializing database:', error);
        throw error;
    }
}

export async function insertCaregivers(caregivers: Caregiver[]): Promise<void> {
    const client = await pool.connect();
    const BATCH_SIZE = 1000; // Insert 1000 records per query
    
    try {
        await client.query('BEGIN');
        
        for (let i = 0; i < caregivers.length; i += BATCH_SIZE) {
            const batch = caregivers.slice(i, i + BATCH_SIZE);
            
            // Build the VALUES clause with placeholders
            const placeholders = batch.map((_, index) => {
                const base = index * 18; // 18 columns
                return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12}, $${base + 13}, $${base + 14}, $${base + 15}, $${base + 16}, $${base + 17}, $${base + 18})`;
            }).join(', ');
            
            // Flatten all values for this batch
            const values = batch.flatMap(caregiver => [
                caregiver.franchisor_id,
                caregiver.agency_id,
                caregiver.subdomain,
                caregiver.profile_id,
                caregiver.caregiver_id,
                caregiver.external_id,
                caregiver.first_name,
                caregiver.last_name,
                caregiver.email,
                caregiver.phone_number,
                caregiver.gender,
                caregiver.applicant,
                caregiver.birthday_date ? caregiver.birthday_date.toISOString().split('T')[0] : null,
                caregiver.onboarding_date ? caregiver.onboarding_date.toISOString() : null,
                caregiver.location_name,
                caregiver.locations_id,
                caregiver.applicant_status,
                caregiver.status
            ]);
            
            const query = `
                INSERT INTO caregivers (
                    franchisor_id, agency_id, subdomain, profile_id, caregiver_id, external_id,
                    first_name, last_name, email, phone_number, gender, applicant,
                    birthday_date, onboarding_date, location_name, locations_id, applicant_status, status
                ) VALUES ${placeholders}
                ON CONFLICT (profile_id) DO NOTHING
            `;
            
            await client.query(query, values);
        }
        
        await client.query('COMMIT');
    } catch (err) {
        await client.query('ROLLBACK');
        console.error('Error inserting caregivers:', err);
        throw err;
    } finally {
        client.release();
    }
}



export async function insertCarelogs(carelogs: Carelog[]): Promise<void> {
    const client = await pool.connect();
    const BATCH_SIZE = 1000;
    
    try {
        await client.query('BEGIN');
        
        for (let i = 0; i < carelogs.length; i += BATCH_SIZE) {
            const batch = carelogs.slice(i, i + BATCH_SIZE);
            
            // Build the VALUES clause with placeholders
            const placeholders = batch.map((_, index) => {
                const base = index * 15; // 15 columns
                return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12}, $${base + 13}, $${base + 14}, $${base + 15})`;
            }).join(', ');
            
            // Flatten all values for this batch
            const values = batch.flatMap(carelog => [
                carelog.franchisor_id,
                carelog.agency_id,
                carelog.carelog_id,
                carelog.caregiver_id,
                carelog.parent_id,
                carelog.start_datetime.toISOString(),
                carelog.end_datetime.toISOString(),
                carelog.clock_in_actual_datetime ? carelog.clock_in_actual_datetime.toISOString() : null,
                carelog.clock_out_actual_datetime ? carelog.clock_out_actual_datetime.toISOString() : null,
                carelog.clock_in_method,
                carelog.clock_out_method,
                carelog.status,
                carelog.split,
                carelog.documentation,
                carelog.general_comment_char_count
            ]);
            
            const query = `
                INSERT INTO carelogs (
                    franchisor_id, agency_id, carelog_id, caregiver_id, parent_id,
                    start_datetime, end_datetime, clock_in_actual_datetime, clock_out_actual_datetime,
                    clock_in_method, clock_out_method, status, split, documentation, general_comment_char_count
                ) VALUES ${placeholders}
                ON CONFLICT (carelog_id) DO NOTHING
            `;
            
            await client.query(query, values);
        }
        
        await client.query('COMMIT');
    } catch (err) {
        await client.query('ROLLBACK');
        console.error('Error inserting carelogs:', err);
        throw err;
    } finally {
        client.release();
    }

}

export async function closeDatabasePool(): Promise<void> {
    await pool.end();
    console.log('PostgreSQL connection pool closed.');
}

