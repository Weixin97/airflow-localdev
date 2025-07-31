
  
    

  create  table "spacex_db"."gold"."starlink_constellation_status__dbt_tmp"
  
  
    as
  
  (
    

SELECT 
    CURRENT_TIMESTAMP as status_date,
    
    -- Current constellation
    COUNT(*) as total_satellites,
    COUNT(CASE WHEN is_active THEN 1 END) as active_satellites,
    COUNT(CASE WHEN NOT is_active THEN 1 END) as inactive_satellites,
    
    -- Performance
    ROUND((COUNT(CASE WHEN is_active THEN 1 END) * 100.0 / COUNT(*))::numeric, 1) as active_percentage,
    
    -- Timeline
    MIN(launch_date) as first_launch_date,
    MAX(launch_date) as latest_launch_date,
    
    -- Data quality
    MAX(extracted_at) as last_updated,
    ROUND((EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(extracted_at))) / 3600)::numeric, 1) as data_age_hours

FROM "spacex_db"."silver"."silver_starlink"
WHERE satellite_id IS NOT NULL
  );
  