
  create view "spacex_db"."gold_silver"."silver_starlink__dbt_tmp"
    
    
  as (
    

WITH parsed_starlink AS (
    SELECT 
        -- Basic identifiers
        data->>'id' as satellite_id,
        data->'spaceTrack'->>'OBJECT_NAME' as satellite_name,
        data->>'launch' as launch_id,
        
        -- Launch and creation dates
        CASE 
            WHEN data->'spaceTrack'->>'LAUNCH_DATE' IS NOT NULL 
            THEN (data->'spaceTrack'->>'LAUNCH_DATE')::date 
            ELSE NULL 
        END as launch_date,
        
        CASE 
            WHEN data->'spaceTrack'->>'CREATION_DATE' IS NOT NULL 
            THEN (data->'spaceTrack'->>'CREATION_DATE')::timestamp 
            ELSE NULL 
        END as tracking_creation_date,
        
        -- Satellite status determination (multiple fallback methods)
        CASE 
            -- Primary: DECAYED field
            WHEN data->'spaceTrack'->>'DECAYED' = '0' THEN true
            WHEN data->'spaceTrack'->>'DECAYED' = 'false' THEN true
            WHEN data->'spaceTrack'->>'DECAYED' = 'False' THEN true
            WHEN data->'spaceTrack'->>'DECAYED' = '1' THEN false
            WHEN data->'spaceTrack'->>'DECAYED' = 'true' THEN false
            WHEN data->'spaceTrack'->>'DECAYED' = 'True' THEN false
            -- Fallback: DECAY_DATE field
            WHEN data->'spaceTrack'->>'DECAY_DATE' IS NULL OR data->'spaceTrack'->>'DECAY_DATE' = '' THEN true
            ELSE false
        END as is_active,
        
        -- Orbital parameters (parsed to numeric types)
        CASE 
            WHEN data->'spaceTrack'->>'INCLINATION' IS NOT NULL 
            THEN (data->'spaceTrack'->>'INCLINATION')::numeric 
            ELSE NULL 
        END as inclination_deg,
        
        CASE 
            WHEN data->'spaceTrack'->>'SEMIMAJOR_AXIS' IS NOT NULL 
            THEN (data->'spaceTrack'->>'SEMIMAJOR_AXIS')::numeric 
            ELSE NULL 
        END as semimajor_axis_km,
        
        CASE 
            WHEN data->'spaceTrack'->>'PERIOD' IS NOT NULL 
            THEN (data->'spaceTrack'->>'PERIOD')::numeric 
            ELSE NULL 
        END as orbital_period_min,
        
        CASE 
            WHEN data->'spaceTrack'->>'APOAPSIS' IS NOT NULL 
            THEN (data->'spaceTrack'->>'APOAPSIS')::numeric 
            ELSE NULL 
        END as apoapsis_km,
        
        CASE 
            WHEN data->'spaceTrack'->>'PERIAPSIS' IS NOT NULL 
            THEN (data->'spaceTrack'->>'PERIAPSIS')::numeric 
            ELSE NULL 
        END as periapsis_km,
        
        -- Current position data (if available)
        CASE 
            WHEN data->>'longitude' IS NOT NULL 
            THEN (data->>'longitude')::numeric 
            ELSE NULL 
        END as longitude,
        
        CASE 
            WHEN data->>'latitude' IS NOT NULL 
            THEN (data->>'latitude')::numeric 
            ELSE NULL 
        END as latitude,
        
        CASE 
            WHEN data->>'height_km' IS NOT NULL 
            THEN (data->>'height_km')::numeric 
            ELSE NULL 
        END as altitude_km,
        
        CASE 
            WHEN data->>'velocity_kms' IS NOT NULL 
            THEN (data->>'velocity_kms')::numeric 
            ELSE NULL 
        END as velocity_kms,
        
        -- Tracking information
        data->'spaceTrack'->>'NORAD_CAT_ID' as norad_catalog_id,
        data->'spaceTrack'->>'OBJECT_ID' as object_id,
        data->'spaceTrack'->>'DECAY_DATE' as decay_date_raw,
        
        -- Data quality assessment
        CASE 
            WHEN data->'spaceTrack'->>'OBJECT_NAME' IS NULL THEN 'missing_satellite_name'
            WHEN data->>'launch' IS NULL THEN 'missing_launch_reference'
            WHEN data->'spaceTrack'->>'DECAYED' IS NULL AND data->'spaceTrack'->>'DECAY_DATE' IS NULL THEN 'missing_status_info'
            WHEN data->'spaceTrack'->>'LAUNCH_DATE' IS NULL THEN 'missing_launch_date'
            ELSE 'valid'
        END as data_quality_flag,
        
        -- Metadata
        extracted_at,
        source_system
        
    FROM "spacex_db"."bronze"."starlink"
    WHERE data IS NOT NULL
)

SELECT 
    satellite_id,
    satellite_name,
    launch_id,
    launch_date,
    tracking_creation_date,
    is_active,
    inclination_deg,
    semimajor_axis_km,
    orbital_period_min,
    apoapsis_km,
    periapsis_km,
    longitude,
    latitude,
    altitude_km,
    velocity_kms,
    norad_catalog_id,
    object_id,
    decay_date_raw,
    data_quality_flag,
    extracted_at,
    source_system,
    
    -- Additional derived fields
    CASE 
        WHEN semimajor_axis_km IS NOT NULL 
        THEN semimajor_axis_km - 6371.0  -- Convert to altitude above Earth's surface
        ELSE NULL 
    END as calculated_altitude_km,
    
    -- Orbital regime classification
    CASE 
        WHEN semimajor_axis_km - 6371.0 BETWEEN 160 AND 2000 THEN 'Low Earth Orbit (LEO)'
        WHEN semimajor_axis_km - 6371.0 BETWEEN 2000 AND 35786 THEN 'Medium Earth Orbit (MEO)'
        WHEN semimajor_axis_km - 6371.0 > 35786 THEN 'Geostationary Orbit (GEO)'
        ELSE 'Unknown'
    END as orbital_regime,
    
    -- Age calculation
    CASE 
        WHEN launch_date IS NOT NULL 
        THEN CURRENT_DATE - launch_date 
        ELSE NULL 
    END as satellite_age_days

FROM parsed_starlink
WHERE satellite_id IS NOT NULL

-- Add model documentation

  );