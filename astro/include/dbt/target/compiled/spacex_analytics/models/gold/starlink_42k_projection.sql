

WITH current_status AS (
    SELECT 
        COUNT(CASE WHEN is_active THEN 1 END) as current_satellites
    FROM "spacex_db"."silver"."silver_starlink"
    WHERE satellite_id IS NOT NULL
),

launch_stats AS (
    SELECT 
        MIN(launch_date) as first_launch_date,
        MAX(launch_date) as latest_launch_date,
        
        COUNT(*) as total_launches,
        COUNT(CASE WHEN launch_success THEN 1 END) as successful_launches,
        (AVG(CASE WHEN launch_success THEN estimated_satellite_count END))::numeric as avg_sats_per_launch,
        
        -- Count launches in 2022 (most recent complete year)
        COUNT(CASE WHEN EXTRACT(YEAR FROM launch_date) = 2022 AND launch_success THEN 1 END) as launches_2022,
        
        -- Calculate monthly rate - cast everything to numeric
        CASE 
            WHEN MAX(launch_date) > MIN(launch_date) THEN
                (COUNT(CASE WHEN launch_success THEN 1 END)::numeric) / 
                ((DATE_PART('year', MAX(launch_date)) - DATE_PART('year', MIN(launch_date)))::numeric * 12.0)
            ELSE 0::numeric
        END as avg_monthly_launch_rate
        
    FROM "spacex_db"."silver"."silver_launches"
    WHERE is_starlink_mission = true
      AND launch_date IS NOT NULL
)

SELECT 
    CURRENT_TIMESTAMP as analysis_date,
    
    -- Current status
    42000 as target_satellites,
    cs.current_satellites,
    (42000 - cs.current_satellites) as satellites_needed,
    ROUND((cs.current_satellites * 100.0 / 42000)::numeric, 1) as progress_percent,
    
    -- Historical performance
    ls.total_launches,
    ls.successful_launches,
    ROUND(ls.avg_sats_per_launch::numeric, 0) as avg_satellites_per_launch,
    ROUND(ls.avg_monthly_launch_rate::numeric, 2) as avg_monthly_launch_rate,
    ls.launches_2022,
    
    -- Business projections
    CASE 
        WHEN ls.avg_sats_per_launch > 0 THEN
            CEILING((42000 - cs.current_satellites)::numeric / ls.avg_sats_per_launch)
        ELSE NULL
    END as launches_needed,
    
    CASE 
        WHEN ls.avg_monthly_launch_rate > 0 AND ls.avg_sats_per_launch > 0 THEN
            ROUND((CEILING((42000 - cs.current_satellites)::numeric / ls.avg_sats_per_launch) / ls.avg_monthly_launch_rate)::numeric, 1)
        ELSE NULL
    END as months_to_completion,
    
    CASE 
        WHEN ls.avg_monthly_launch_rate > 0 AND ls.avg_sats_per_launch > 0 THEN
            (CURRENT_DATE + (ROUND((CEILING((42000 - cs.current_satellites)::numeric / ls.avg_sats_per_launch) / ls.avg_monthly_launch_rate)::numeric, 1) || ' months')::INTERVAL)::date
        ELSE NULL
    END as estimated_completion_date,
    
    -- Confidence assessment
    CASE 
        WHEN ls.launches_2022 >= 5 THEN 'Medium Confidence'
        WHEN ls.total_launches >= 20 THEN 'Low Confidence'
        ELSE 'Insufficient Data'
    END as projection_confidence,
    
    -- Data context
    ls.first_launch_date,
    ls.latest_launch_date,
    ('Projection based on ' || ls.successful_launches || ' successful launches from ' || 
    EXTRACT(YEAR FROM ls.first_launch_date) || ' to ' || EXTRACT(YEAR FROM ls.latest_launch_date)) as data_summary

FROM current_status cs
CROSS JOIN launch_stats ls