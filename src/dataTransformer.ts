import type { Caregiver, CaregiverCsvRow, Carelog, CarelogCsvRow } from "./types.js";


export function transformCaregivers(csvRows: CaregiverCsvRow[]): Caregiver[] {
    const transformedCaregivers: Caregiver[] = [];
    
    for (const row of csvRows) {
        try {
            const transformed: Caregiver = {
                franchisor_id: row.franchisor_id,
                agency_id: row.agency_id,
                subdomain: row.subdomain || null,
                profile_id: row.profile_id,
                caregiver_id: row.caregiver_id,
                external_id: row.external_id || null,
                first_name: row.first_name || null,
                last_name: row.last_name || null,
                email: row.email || null,
                phone_number: row.phone_number || null,
                gender: row.gender || null,
                applicant: row.applicant === 'True' ? true : row.applicant === 'False' ? false : null,
                birthday_date: row.birthday_date ? parseDate(row.birthday_date) : null,
                onboarding_date: row.onboarding_date ? parseDate(row.onboarding_date) : null,
                location_name: row.location_name === 'None' ? null : row.location_name || null,
                locations_id: row.locations_id && row.locations_id !== '0' ? parseInt(row.locations_id) : null,
                applicant_status: row.applicant_status || null,
                status: row.status
            };
            
            transformedCaregivers.push(transformed);
        } catch (error) {
            // console.warn(`Skipping invalid caregiver row: ${JSON.stringify(row)}. Error: ${error}`);
        }
    }
    
    console.log(`Transformed ${transformedCaregivers.length} caregivers out of ${csvRows.length} rows`);
    return transformedCaregivers;
}

export function transformCarelogs(csvRows: CarelogCsvRow[]): Carelog[] {
    const transformedCarelogs: Carelog[] = [];
    
    for (const row of csvRows) {
        try {
            const transformed: Carelog = {
                franchisor_id: row.franchisor_id,
                agency_id: row.agency_id,
                carelog_id: row.carelog_id,
                caregiver_id: row.caregiver_id,
                parent_id: row.parent_id || null,
                start_datetime: parseDate(row.start_datetime),
                end_datetime: parseDate(row.end_datetime),
                clock_in_actual_datetime: row.clock_in_actual_datetime ? parseDate(row.clock_in_actual_datetime) : null,
                clock_out_actual_datetime: row.clock_out_actual_datetime ? parseDate(row.clock_out_actual_datetime) : null,
                clock_in_method: row.clock_in_method ? parseInt(row.clock_in_method) : null,
                clock_out_method: row.clock_out_method ? parseInt(row.clock_out_method) : null,
                status: row.status ? parseInt(row.status) : null,
                split: row.split === 'True' ? true : row.split === 'False' ? false : null,
                documentation: row.documentation || null,
                general_comment_char_count: row.general_comment_char_count ? parseInt(row.general_comment_char_count) : null
            };
            

            if (!transformed.start_datetime || !transformed.end_datetime) {
                throw new Error('Missing required datetime fields');
            }
            
            
            transformedCarelogs.push(transformed);
        } catch (error) {
            // console.warn(`Skipping invalid carelog row: ${JSON.stringify(row)}. Error: ${error}`);
        }
    }
    
    console.log(`Transformed ${transformedCarelogs.length} carelogs out of ${csvRows.length} rows`);
    return transformedCarelogs;
}



function parseDate(dateString: string): Date | null {
    if (!dateString || dateString.trim() === '' || dateString === 'None') {
        return null;
    }
    
    const date = new Date(dateString);
    if (isNaN(date.getTime())) {
        throw new Error(`Invalid date: ${dateString}`);
    }
    
    return date;
}
