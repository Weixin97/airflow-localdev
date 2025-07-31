
  create view "spacex_db"."silver"."silver_launches__dbt_tmp"
    
    
  as (
    

WITH parsed_launches AS (
    SELECT 
        -- Basic identifiers
        data->>'id' as launch_id,
        data->>'name' as mission_name,
        
        -- Date and time parsing
        CASE 
            WHEN data->>'date_utc' IS NOT NULL 
            THEN (data->>'date_utc')::timestamp 
            ELSE NULL 
        END as launch_date_utc,
        
        CASE 
            WHEN data->>'date_utc' IS NOT NULL 
            THEN (data->>'date_utc')::timestamp::date 
            ELSE NULL 
        END as launch_date,
        
        -- Launch outcome
        CASE 
            WHEN (data->'success')::text = 'true' THEN true
            WHEN (data->'success')::text = 'false' THEN false
            ELSE NULL
        END as launch_success,
        
        CASE 
            WHEN (data->'upcoming')::text = 'true' THEN true
            WHEN (data->'upcoming')::text = 'false' THEN false
            ELSE NULL
        END as is_upcoming,
        
        -- Technical details
        data->>'rocket' as rocket_id,
        
        CASE 
            WHEN data->>'flight_number' IS NOT NULL 
            THEN (data->>'flight_number')::integer 
            ELSE NULL 
        END as flight_number,
        
        data->>'launchpad' as launchpad_id,
        data->>'details' as mission_details,
        
        -- Business logic: Starlink mission identification
        CASE 
            WHEN LOWER(data->>'name') LIKE '%starlink%' THEN true
            WHEN LOWER(data->>'name') LIKE '%starship%' AND LOWER(data->>'name') LIKE '%starlink%' THEN true
            ELSE false
        END as is_starlink_mission,
        
        -- Satellite count estimation for Starlink missions
        CASE 
            WHEN LOWER(data->>'name') LIKE '%starlink%' THEN
                CASE 
                    -- Early test missions
                    WHEN LOWER(data->>'name') LIKE '%demo%' 
                      OR LOWER(data->>'name') LIKE '%test%' 
                      OR data->>'name' ILIKE '%tintin%' THEN 2
                    -- V1.0 missions (60 satellites each)
                    WHEN data->>'name' ILIKE '%v1.0%' THEN 60
                    -- V2 missions (larger satellites, fewer per launch)
                    WHEN data->>'name' ILIKE '%v2%' THEN 23
                    -- Standard production batches
                    ELSE 60
                END
            ELSE 0
        END as estimated_satellite_count,
        
        -- Payload analysis
        CASE 
            WHEN data->'payloads' IS NOT NULL 
            THEN jsonb_array_length(data->'payloads') 
            ELSE 0 
        END as payload_count,
        
        -- Data quality assessment
        CASE 
            WHEN data->>'date_utc' IS NULL THEN 'missing_launch_date'
            WHEN data->>'name' IS NULL THEN 'missing_mission_name'
            WHEN (data->'success') IS NULL AND (data->'upcoming')::text = 'false' THEN 'missing_success_status'
            WHEN data->>'rocket' IS NULL THEN 'missing_rocket_info'
            ELSE 'valid'
        END as data_quality_flag,
        
        -- Metadata
        extracted_at,
        source_system
        
    FROM "spacex_db"."bronze"."launches"
    WHERE data IS NOT NULL
)

SELECT 
    launch_id,
    mission_name,
    launch_date_utc,
    launch_date,
    launch_success,
    is_upcoming,
    rocket_id,
    flight_number,
    launchpad_id,
    mission_details,
    is_starlink_mission,
    estimated_satellite_count,
    payload_count,
    data_quality_flag,
    extracted_at,
    source_system,
    
    -- Additional derived fields
    EXTRACT(YEAR FROM launch_date_utc) as launch_year,
    EXTRACT(MONTH FROM launch_date_utc) as launch_month,
    EXTRACT(DOW FROM launch_date_utc) as launch_day_of_week,
    
    -- Mission type classification
    CASE 
        WHEN is_starlink_mission THEN 'Starlink'
        WHEN LOWER(mission_name) LIKE '%crew%' THEN 'Crew'
        WHEN LOWER(mission_name) LIKE '%cargo%' OR LOWER(mission_name) LIKE '%crs%' THEN 'Cargo'
        WHEN LOWER(mission_name) LIKE '%satellite%' THEN 'Commercial Satellite'
        ELSE 'Other'
    END as mission_type

FROM parsed_launches
WHERE launch_id IS NOT NULL

-- Add model documentation

  );