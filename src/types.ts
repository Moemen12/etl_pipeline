export interface CaregiverCsvRow {
    franchisor_id: string;
    agency_id: string;
    subdomain: string;
    profile_id: string;
    caregiver_id: string;
    external_id: string;
    first_name: string;
    last_name: string;
    email: string;
    phone_number: string;
    gender: string;
    applicant: string;
    birthday_date: string;
    onboarding_date: string;
    location_name: string;
    locations_id: string;
    applicant_status: string;
    status: string;
}

export interface CarelogCsvRow {
    franchisor_id: string;
    agency_id: string;
    carelog_id: string;
    caregiver_id: string;
    parent_id: string;
    start_datetime: string;
    end_datetime: string;
    clock_in_actual_datetime: string;
    clock_out_actual_datetime: string;
    clock_in_method: string;
    clock_out_method: string;
    status: string;
    split: string; 
    documentation: string;
    general_comment_char_count: string;
}


export interface Caregiver {
    franchisor_id: string;
    agency_id: string;
    subdomain: string | null;
    profile_id: string;
    caregiver_id: string;
    external_id: string | null;
    first_name: string | null;
    last_name: string | null;
    email: string | null;
    phone_number: string | null;
    gender: string | null;
    applicant: boolean | null;
    birthday_date: Date | null;
    onboarding_date: Date | null;
    location_name: string | null;
    locations_id: number | null;
    applicant_status: string | null;
    status: string;
}

export interface Carelog {
    franchisor_id: string;
    agency_id: string;
    carelog_id: string;
    caregiver_id: string;
    parent_id: string | null;
    start_datetime: Date|  null;
    end_datetime: Date|  null;
    clock_in_actual_datetime: Date | null;
    clock_out_actual_datetime: Date | null;
    clock_in_method: number | null;
    clock_out_method: number | null;
    status: number | null;
    split: boolean | null;
    documentation: string | null;
    general_comment_char_count: number | null;
}
