-- ===================================================================
-- 1. CAREGIVER RELIABILITY & ATTENDANCE
-- ===================================================================

-- Top Performers: Identify caregivers with the highest number of completed visits
-- Definition: "Completed" = visits with both actual clock-in and clock-out times
WITH completed_visits AS (
    SELECT 
        c.caregiver_id,
        c.first_name,
        c.last_name,
        c.agency_id,
        COUNT(*) as total_visits,
        COUNT(CASE WHEN cl.clock_in_actual_datetime IS NOT NULL 
                   AND cl.clock_out_actual_datetime IS NOT NULL THEN 1 END) as completed_visits
    FROM caregivers c
    JOIN carelogs cl ON c.caregiver_id = cl.caregiver_id
    GROUP BY c.caregiver_id, c.first_name, c.last_name, c.agency_id
)
SELECT 
    caregiver_id,
    first_name || ' ' || last_name as caregiver_name,
    agency_id,
    completed_visits,
    total_visits,
    ROUND(completed_visits * 100.0 / total_visits, 2) as completion_rate_percent
FROM completed_visits
ORDER BY completed_visits DESC;

-- Reliability Issues: Highlight caregivers showing frequent reliability issues
-- Criteria: late arrivals, cancellations, missed visits
WITH reliability_analysis AS (
    SELECT 
        c.caregiver_id,
        c.first_name || ' ' || c.last_name as caregiver_name,
        c.agency_id,
        COUNT(*) as total_scheduled_visits,
        
        -- Missed visits: scheduled but no actual clock-in
        COUNT(CASE WHEN cl.clock_in_actual_datetime IS NULL THEN 1 END) as missed_visits,
        
        -- Late arrivals: actual clock-in significantly after scheduled start (>30 minutes)
        COUNT(CASE WHEN cl.clock_in_actual_datetime > cl.start_datetime + INTERVAL '30 minutes' THEN 1 END) as late_arrivals,
        
        -- Cancellations: assuming status codes indicate cancellations
        COUNT(CASE WHEN cl.status IN (0, -1) THEN 1 END) as cancellations
        
    FROM caregivers c
    JOIN carelogs cl ON c.caregiver_id = cl.caregiver_id
    GROUP BY c.caregiver_id, c.first_name, c.last_name, c.agency_id
)
SELECT 
    caregiver_id,
    caregiver_name,
    agency_id,
    total_scheduled_visits,
    missed_visits,
    late_arrivals,
    cancellations,
    (missed_visits + late_arrivals + cancellations) as total_reliability_issues,
    ROUND((missed_visits + late_arrivals + cancellations) * 100.0 / total_scheduled_visits, 2) as reliability_issue_rate_percent
FROM reliability_analysis
WHERE (missed_visits + late_arrivals + cancellations) > 0
ORDER BY total_reliability_issues DESC, reliability_issue_rate_percent DESC;

-- ===================================================================
-- 2. VISIT DURATION & OPERATIONAL EFFICIENCY
-- ===================================================================

-- Visit Duration Analysis: Calculate and present the average actual duration of caregiver visits
WITH visit_durations AS (
    SELECT 
        cl.carelog_id,
        c.caregiver_id,
        c.first_name || ' ' || c.last_name as caregiver_name,
        c.agency_id,
        
        -- Actual duration in minutes when both clock times exist
        CASE WHEN cl.clock_in_actual_datetime IS NOT NULL AND cl.clock_out_actual_datetime IS NOT NULL
             THEN EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/60
             ELSE NULL
        END as actual_duration_minutes,
        
        -- Handle missing or inconsistent timestamps
        CASE WHEN cl.clock_in_actual_datetime IS NULL OR cl.clock_out_actual_datetime IS NULL
             THEN 'Missing Clock Times'
             WHEN cl.clock_in_actual_datetime >= cl.clock_out_actual_datetime
             THEN 'Inconsistent Times'
             ELSE 'Valid'
        END as timestamp_status
        
    FROM carelogs cl
    JOIN caregivers c ON cl.caregiver_id = c.caregiver_id
)
SELECT 
    caregiver_id,
    caregiver_name,
    agency_id,
    COUNT(*) as total_visits,
    COUNT(CASE WHEN timestamp_status = 'Valid' THEN 1 END) as valid_duration_visits,
    ROUND(AVG(CASE WHEN timestamp_status = 'Valid' THEN actual_duration_minutes END), 2) as avg_actual_duration_minutes,
    COUNT(CASE WHEN timestamp_status = 'Missing Clock Times' THEN 1 END) as missing_timestamps,
    COUNT(CASE WHEN timestamp_status = 'Inconsistent Times' THEN 1 END) as inconsistent_timestamps
FROM visit_durations
GROUP BY caregiver_id, caregiver_name, agency_id
HAVING COUNT(CASE WHEN timestamp_status = 'Valid' THEN 1 END) > 0
ORDER BY avg_actual_duration_minutes DESC;

-- Identifying Outliers: Identify visits significantly shorter or longer than typical durations
WITH duration_stats AS (
    SELECT 
        EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/60 as duration_minutes
    FROM carelogs cl
    WHERE cl.clock_in_actual_datetime IS NOT NULL 
      AND cl.clock_out_actual_datetime IS NOT NULL
      AND cl.clock_in_actual_datetime < cl.clock_out_actual_datetime
),
percentiles AS (
    SELECT 
        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY duration_minutes) as p10,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY duration_minutes) as p90,
        AVG(duration_minutes) as avg_duration
    FROM duration_stats
)
SELECT 
    cl.carelog_id,
    c.first_name || ' ' || c.last_name as caregiver_name,
    c.agency_id,
    ROUND(EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/60, 2) as actual_duration_minutes,
    CASE 
        WHEN EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/60 < p.p10 
        THEN 'Significantly Shorter'
        WHEN EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/60 > p.p90 
        THEN 'Significantly Longer'
    END as duration_category,
    cl.start_datetime,
    ROUND(p.avg_duration, 2) as system_avg_duration
FROM carelogs cl
JOIN caregivers c ON cl.caregiver_id = c.caregiver_id
CROSS JOIN percentiles p
WHERE cl.clock_in_actual_datetime IS NOT NULL 
  AND cl.clock_out_actual_datetime IS NOT NULL
  AND cl.clock_in_actual_datetime < cl.clock_out_actual_datetime
  AND (EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/60 < p.p10 
       OR EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/60 > p.p90)
ORDER BY actual_duration_minutes;

-- ===================================================================
-- 3. DOCUMENTATION PATTERNS & DATA QUALITY
-- ===================================================================

-- Detailed Documentation Providers: Identify caregivers consistently leaving detailed comments
-- Define criteria for "consistent" and "detailed"
WITH documentation_analysis AS (
    SELECT 
        c.caregiver_id,
        c.first_name || ' ' || c.last_name as caregiver_name,
        c.agency_id,
        COUNT(*) as total_visits,
        COUNT(CASE WHEN cl.documentation IS NOT NULL AND LENGTH(TRIM(cl.documentation)) > 0 THEN 1 END) as visits_with_documentation,
        AVG(CASE WHEN cl.documentation IS NOT NULL THEN LENGTH(cl.documentation) ELSE 0 END) as avg_documentation_length
    FROM caregivers c
    JOIN carelogs cl ON c.caregiver_id = cl.caregiver_id
    GROUP BY c.caregiver_id, c.first_name, c.last_name, c.agency_id
)
SELECT 
    caregiver_id,
    caregiver_name,
    agency_id,
    total_visits,
    visits_with_documentation,
    ROUND(visits_with_documentation * 100.0 / total_visits, 2) as documentation_consistency_percent,
    ROUND(avg_documentation_length, 2) as avg_documentation_length,
    CASE 
        WHEN visits_with_documentation * 100.0 / total_visits >= 80 AND avg_documentation_length >= 100 
        THEN 'Consistently Detailed'
        WHEN visits_with_documentation * 100.0 / total_visits >= 80 
        THEN 'Consistent'
        WHEN avg_documentation_length >= 100 
        THEN 'Detailed When Present'
        ELSE 'Needs Improvement'
    END as documentation_quality_category
FROM documentation_analysis
ORDER BY documentation_consistency_percent DESC, avg_documentation_length DESC;

-- Data Quality Check: Highlight unusual or suspicious patterns in documentation data
SELECT 
    'Empty or Missing Documentation' as data_quality_issue,
    COUNT(*) as occurrences,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM carelogs), 2) as percentage_of_total_visits
FROM carelogs 
WHERE documentation IS NULL OR LENGTH(TRIM(documentation)) = 0

UNION ALL

SELECT 
    'Unusually Short Documentation (<10 characters)',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM carelogs), 2)
FROM carelogs 
WHERE documentation IS NOT NULL AND LENGTH(TRIM(documentation)) BETWEEN 1 AND 9

UNION ALL

SELECT 
    'Unusually Long Documentation (>1000 characters)',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM carelogs), 2)
FROM carelogs 
WHERE documentation IS NOT NULL AND LENGTH(documentation) > 1000

UNION ALL

SELECT 
    'Suspicious Repetitive Patterns',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM carelogs), 2)
FROM carelogs 
WHERE documentation LIKE '%performed performed%' 
   OR documentation LIKE '%completed completed%'
   OR documentation LIKE '%provided provided%'

UNION ALL

SELECT 
    'Contains HTML or Special Characters',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM carelogs), 2)
FROM carelogs 
WHERE documentation LIKE '%<br%' 
   OR documentation LIKE '%&%'
   OR documentation LIKE '%<%'

ORDER BY occurrences DESC;

-- ===================================================================
-- 4. CAREGIVER OVERTIME ANALYSIS  
-- ===================================================================

-- Overtime Identification: Identify caregivers regularly incurring overtime hours
-- Define overtime as exceeding 40 hours per week
WITH weekly_hours AS (
    SELECT 
        c.caregiver_id,
        c.first_name || ' ' || c.last_name as caregiver_name,
        c.agency_id,
        DATE_TRUNC('week', cl.start_datetime) as week_start,
        SUM(
            CASE WHEN cl.clock_in_actual_datetime IS NOT NULL AND cl.clock_out_actual_datetime IS NOT NULL
                 THEN EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600
                 ELSE 0
            END
        ) as total_weekly_hours
    FROM caregivers c
    JOIN carelogs cl ON c.caregiver_id = cl.caregiver_id
    GROUP BY c.caregiver_id, c.first_name, c.last_name, c.agency_id, DATE_TRUNC('week', cl.start_datetime)
),
overtime_summary AS (
    SELECT 
        caregiver_id,
        caregiver_name,
        agency_id,
        COUNT(*) as total_weeks_worked,
        COUNT(CASE WHEN total_weekly_hours > 40 THEN 1 END) as weeks_with_overtime,
        AVG(total_weekly_hours) as avg_weekly_hours,
        MAX(total_weekly_hours) as max_weekly_hours,
        SUM(CASE WHEN total_weekly_hours > 40 THEN total_weekly_hours - 40 ELSE 0 END) as total_overtime_hours
    FROM weekly_hours
    GROUP BY caregiver_id, caregiver_name, agency_id
)
SELECT 
    caregiver_id,
    caregiver_name,
    agency_id,
    total_weeks_worked,
    weeks_with_overtime,
    ROUND(weeks_with_overtime * 100.0 / total_weeks_worked, 2) as overtime_frequency_percent,
    ROUND(avg_weekly_hours, 2) as avg_weekly_hours,
    ROUND(max_weekly_hours, 2) as max_weekly_hours,
    ROUND(total_overtime_hours, 2) as total_overtime_hours
FROM overtime_summary
WHERE weeks_with_overtime > 0
ORDER BY total_overtime_hours DESC, overtime_frequency_percent DESC;

-- Operational Insights: Patterns and insights related to overtime
-- Are specific caregivers or agencies disproportionately responsible for overtime?
WITH agency_overtime_analysis AS (
    SELECT 
        c.agency_id,
        COUNT(DISTINCT c.caregiver_id) as total_caregivers,
        SUM(
            CASE WHEN cl.clock_in_actual_datetime IS NOT NULL AND cl.clock_out_actual_datetime IS NOT NULL
                 THEN EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600
                 ELSE 0
            END
        ) as total_hours_worked,
        COUNT(*) as total_visits
    FROM caregivers c
    JOIN carelogs cl ON c.caregiver_id = cl.caregiver_id
    GROUP BY c.agency_id
),
weekly_agency_overtime AS (
    SELECT 
        c.agency_id,
        DATE_TRUNC('week', cl.start_datetime) as week_start,
        SUM(
            CASE WHEN cl.clock_in_actual_datetime IS NOT NULL AND cl.clock_out_actual_datetime IS NOT NULL
                 THEN EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600
                 ELSE 0
            END
        ) as weekly_total_hours
    FROM caregivers c
    JOIN carelogs cl ON c.caregiver_id = cl.caregiver_id
    GROUP BY c.agency_id, DATE_TRUNC('week', cl.start_datetime)
)
SELECT 
    aoa.agency_id,
    aoa.total_caregivers,
    ROUND(aoa.total_hours_worked, 2) as total_hours_worked,
    aoa.total_visits,
    ROUND(aoa.total_hours_worked / aoa.total_caregivers, 2) as avg_hours_per_caregiver,
    COUNT(CASE WHEN wao.weekly_total_hours > (40 * aoa.total_caregivers) THEN 1 END) as weeks_with_agency_overtime
FROM agency_overtime_analysis aoa
LEFT JOIN weekly_agency_overtime wao ON aoa.agency_id = wao.agency_id
GROUP BY aoa.agency_id, aoa.total_caregivers, aoa.total_hours_worked, aoa.total_visits
ORDER BY avg_hours_per_caregiver DESC;

-- Do certain schedules or visit types correlate with higher overtime?
WITH visit_timing_analysis AS (
    SELECT 
        c.caregiver_id,
        EXTRACT(DOW FROM cl.start_datetime) as day_of_week,
        EXTRACT(HOUR FROM cl.start_datetime) as hour_of_day,
        CASE WHEN cl.clock_in_actual_datetime IS NOT NULL AND cl.clock_out_actual_datetime IS NOT NULL
             THEN EXTRACT(EPOCH FROM (cl.clock_out_actual_datetime - cl.clock_in_actual_datetime))/3600
             ELSE 0
        END as visit_duration_hours
    FROM caregivers c
    JOIN carelogs cl ON c.caregiver_id = cl.caregiver_id
    WHERE cl.clock_in_actual_datetime IS NOT NULL AND cl.clock_out_actual_datetime IS NOT NULL
)
SELECT 
    CASE day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday' 
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_of_week,
    CASE 
        WHEN hour_of_day BETWEEN 6 AND 11 THEN 'Morning (6-11)'
        WHEN hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
        WHEN hour_of_day BETWEEN 18 AND 22 THEN 'Evening (18-22)'
        ELSE 'Night/Early (23-5)'
    END as time_period,
    COUNT(*) as total_visits,
    ROUND(AVG(visit_duration_hours), 2) as avg_visit_duration_hours,
    COUNT(CASE WHEN visit_duration_hours > 8 THEN 1 END) as long_visits_over_8hrs
FROM visit_timing_analysis
GROUP BY day_of_week, 
         CASE 
             WHEN hour_of_day BETWEEN 6 AND 11 THEN 'Morning (6-11)'
             WHEN hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
             WHEN hour_of_day BETWEEN 18 AND 22 THEN 'Evening (18-22)'
             ELSE 'Night/Early (23-5)'
         END
ORDER BY day_of_week, time_period;