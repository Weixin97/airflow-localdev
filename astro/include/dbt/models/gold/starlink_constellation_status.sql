{{ config(
    materialized='table',
    description='Current Starlink constellation status and key performance metrics'
) }}

WITH constellation_metrics AS (
    SELECT 
        -- Current constellation status
        COUNT(*) as total_satellites_tracked,
        COUNT(CASE WHEN is_active THEN 1 END) as current_active_satellites,
        COUNT(CASE WHEN NOT is_active THEN 1 END) as inactive_satellites,
        ROUND(
            COUNT(CASE WHEN is_active THEN 1 END) * 100.0 / COUNT(*), 
            2
        ) as active_percentage,
        
        -- Timeline information
        MIN(launch_date) as first_satellite_launch_date,
        MAX(launch_date) as latest_satellite_launch_date,
        
        -- Orbital characteristics (for active satellites only)
        ROUND(AVG(CASE WHEN is_active THEN inclination_deg END), 2) as avg_inclination_active_sats,
        ROUND(AVG(CASE WHEN is_active THEN calculated_altitude_km END), 1) as avg_altitude_active_sats_km,
        ROUND(AVG(CASE WHEN is_active THEN orbital_period_min END), 2) as avg_period_active_sats_min,
        
        -- Quality metrics
        COUNT(CASE WHEN data_quality_flag != 'valid' THEN 1 END) as satellites_with_quality_issues,
        ROUND(
            COUNT(CASE WHEN data_quality_flag = 'valid' THEN 1 END) * 100.0 / COUNT(*),
            2
        ) as data_quality_percentage

    FROM {{ ref('silver_starlink') }}
    WHERE satellite_id IS NOT NULL
),

launch_performance AS (
    SELECT 
        -- Launch statistics (Starlink missions only)
        COUNT(*) as total_starlink_launches,
        COUNT(CASE WHEN launch_success THEN 1 END) as successful_starlink_launches,
        COUNT(CASE WHEN NOT launch_success AND NOT is_upcoming THEN 1 END) as failed_starlink_launches,
        ROUND(
            COUNT(CASE WHEN launch_success THEN 1 END) * 100.0 / 
            COUNT(CASE WHEN NOT is_upcoming THEN 1 END), 
            2
        ) as launch_success_rate_pct,
        
        -- Satellite deployment metrics
        SUM(CASE WHEN launch_success THEN estimated_satellite_count ELSE 0 END) as satellites_launched_successfully,
        ROUND(
            AVG(CASE WHEN launch_success THEN estimated_satellite_count END), 
            1
        ) as avg_satellites_per_successful_launch,
        
        -- Timeline
        MIN(launch_date) as first_starlink_launch_date,
        MAX(CASE WHEN NOT is_upcoming THEN launch_date END) as latest_completed_launch_date,
        
        -- Recent activity (last 30 days)
        COUNT(CASE WHEN launch_date >= CURRENT_DATE - INTERVAL '30 days' AND launch_success THEN 1 END) as launches_last_30_days,
        SUM(CASE WHEN launch_date >= CURRENT_DATE - INTERVAL '30 days' AND launch_success THEN estimated_satellite_count ELSE 0 END) as satellites_deployed_last_30_days

    FROM {{ ref('silver_launches') }}
    WHERE is_starlink_mission = true
      AND data_quality_flag = 'valid'
),

data_freshness AS (
    SELECT 
        MAX(extracted_at) as last_data_update,
        ROUND((EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(extracted_at))) / 3600)::numeric, 2) as hours_since_last_update
    FROM {{ ref('silver_starlink') }}
)

SELECT 
    'starlink_constellation' as metric_category,
    CURRENT_TIMESTAMP as calculated_at,
    
    -- Constellation metrics
    c.total_satellites_tracked,
    c.current_active_satellites,
    c.inactive_satellites,
    c.active_percentage,
    c.satellites_with_quality_issues,
    c.data_quality_percentage,
    
    -- Launch performance metrics
    l.total_starlink_launches,
    l.successful_starlink_launches,
    l.failed_starlink_launches,
    l.launch_success_rate_pct,
    l.satellites_launched_successfully,
    l.avg_satellites_per_successful_launch,
    l.launches_last_30_days,
    l.satellites_deployed_last_30_days,
    
    -- Timeline information
    l.first_starlink_launch_date,
    l.latest_completed_launch_date,
    c.first_satellite_launch_date,
    c.latest_satellite_launch_date,
    
    -- Technical averages for active satellites
    c.avg_inclination_active_sats as avg_inclination_deg,
    c.avg_altitude_active_sats_km as avg_altitude_km,
    c.avg_period_active_sats_min as avg_orbital_period_min,
    
    -- Data freshness indicators
    f.last_data_update,
    ROUND(f.hours_since_last_update::numeric, 1) as hours_since_last_update,
    
    -- Health indicators
    CASE 
        WHEN f.hours_since_last_update > 48 THEN 'Stale'
        WHEN f.hours_since_last_update > 24 THEN 'Warning'
        ELSE 'Fresh'
    END as data_freshness_status,
    
    CASE 
        WHEN c.data_quality_percentage >= 95 THEN 'Excellent'
        WHEN c.data_quality_percentage >= 90 THEN 'Good'
        WHEN c.data_quality_percentage >= 80 THEN 'Fair'
        ELSE 'Poor'
    END as data_quality_status

FROM constellation_metrics c
CROSS JOIN launch_performance l
CROSS JOIN data_freshness f