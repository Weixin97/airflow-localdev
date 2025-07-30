{{ config(
    materialized='table',
    description='Business analysis: Projection of when Starlink will reach 42,000 satellites and required launches'
) }}

WITH current_status AS (
    SELECT * FROM {{ ref('starlink_constellation_status') }}
),

recent_launch_frequency AS (
    -- Calculate launch frequency based on recent performance (last 24 months for accuracy)
    SELECT 
        COUNT(*) as launches_last_24_months,
        COUNT(*) / 24.0 as avg_launches_per_month,
        COUNT(*) / 2.0 as avg_launches_per_year,
        
        -- Seasonal analysis (launches by quarter)
        AVG(CASE WHEN EXTRACT(QUARTER FROM launch_date) = 1 THEN 1.0 ELSE 0.0 END) * 12 as q1_annualized_rate,
        AVG(CASE WHEN EXTRACT(QUARTER FROM launch_date) = 2 THEN 1.0 ELSE 0.0 END) * 12 as q2_annualized_rate,
        AVG(CASE WHEN EXTRACT(QUARTER FROM launch_date) = 3 THEN 1.0 ELSE 0.0 END) * 12 as q3_annualized_rate,
        AVG(CASE WHEN EXTRACT(QUARTER FROM launch_date) = 4 THEN 1.0 ELSE 0.0 END) * 12 as q4_annualized_rate,
        
        -- Recent trend (last 6 months vs previous 6 months)
        COUNT(CASE WHEN launch_date >= CURRENT_DATE - INTERVAL '6 months' THEN 1 END) / 6.0 as recent_6mo_monthly_rate,
        COUNT(CASE WHEN launch_date BETWEEN CURRENT_DATE - INTERVAL '12 months' AND CURRENT_DATE - INTERVAL '6 months' THEN 1 END) / 6.0 as previous_6mo_monthly_rate

    FROM {{ ref('silver_launches') }}
    WHERE is_starlink_mission = true
      AND launch_success = true
      AND launch_date >= CURRENT_DATE - INTERVAL '24 months'
      AND launch_date <= CURRENT_DATE  -- Exclude future/upcoming launches
),

deployment_efficiency AS (
    -- Analyze satellite deployment efficiency over time
    SELECT 
        -- Actual satellites deployed vs estimated
        AVG(estimated_satellite_count) as avg_estimated_per_launch,
        
        -- Success rate by time period
        COUNT(CASE WHEN launch_date >= CURRENT_DATE - INTERVAL '12 months' AND launch_success THEN 1 END) as recent_successful_launches,
        COUNT(CASE WHEN launch_date >= CURRENT_DATE - INTERVAL '12 months' THEN 1 END) as recent_total_launches,
        
        -- Calculate actual deployment rate (using Starlink satellite counts)
        CASE 
            WHEN COUNT(CASE WHEN launch_success THEN 1 END) > 0 THEN
                (SELECT current_active_satellites FROM current_status) / COUNT(CASE WHEN launch_success THEN 1 END)
            ELSE NULL
        END as actual_satellites_per_successful_launch

    FROM {{ ref('silver_launches') }}
    WHERE is_starlink_mission = true
      AND launch_date >= CURRENT_DATE - INTERVAL '24 months'
),

projection_scenarios AS (
    SELECT 
        s.*,
        f.launches_last_24_months,
        f.avg_launches_per_month,
        f.avg_launches_per_year,
        f.recent_6mo_monthly_rate,
        f.previous_6mo_monthly_rate,
        e.avg_estimated_per_launch,
        e.recent_successful_launches,
        e.recent_total_launches,
        e.actual_satellites_per_successful_launch,
        
        -- Business question parameters
        42000 as target_satellite_count,
        42000 - s.current_active_satellites as satellites_still_needed,
        
        -- Scenario 1: Conservative (based on historical average)
        CASE 
            WHEN GREATEST(COALESCE(e.actual_satellites_per_successful_launch, 0), COALESCE(e.avg_estimated_per_launch, 0)) > 0 THEN
                CEILING((42000 - s.current_active_satellites) / GREATEST(e.actual_satellites_per_successful_launch, e.avg_estimated_per_launch))
            ELSE NULL
        END as conservative_launches_needed,
        
        -- Scenario 2: Current pace (based on recent 6 months)
        CASE 
            WHEN GREATEST(COALESCE(e.actual_satellites_per_successful_launch, 0), 50) > 0 THEN
                CEILING((42000 - s.current_active_satellites) / GREATEST(e.actual_satellites_per_successful_launch, 50))
            ELSE NULL
        END as current_pace_launches_needed,
        
        -- Scenario 3: Optimistic (assuming improved efficiency)
        CEILING((42000 - s.current_active_satellites) / 70.0) as optimistic_launches_needed,
        
        -- Timeline calculations
        CASE 
            WHEN f.avg_launches_per_month > 0 AND GREATEST(e.actual_satellites_per_successful_launch, e.avg_estimated_per_launch) > 0 THEN
                CEILING((42000 - s.current_active_satellites) / GREATEST(e.actual_satellites_per_successful_launch, e.avg_estimated_per_launch)) / f.avg_launches_per_month
            ELSE NULL
        END as conservative_months_needed,
        
        CASE 
            WHEN f.recent_6mo_monthly_rate > 0 AND GREATEST(e.actual_satellites_per_successful_launch, 50) > 0 THEN
                CEILING((42000 - s.current_active_satellites) / GREATEST(e.actual_satellites_per_successful_launch, 50)) / f.recent_6mo_monthly_rate
            ELSE NULL
        END as current_pace_months_needed,
        
        -- Trend analysis
        CASE 
            WHEN f.recent_6mo_monthly_rate > f.previous_6mo_monthly_rate THEN 'Accelerating'
            WHEN f.recent_6mo_monthly_rate < f.previous_6mo_monthly_rate THEN 'Decelerating'
            ELSE 'Stable'
        END as launch_frequency_trend

    FROM current_status s
    CROSS JOIN recent_launch_frequency f
    CROSS JOIN deployment_efficiency e
)

SELECT 
    'starlink_42k_analysis' as analysis_type,
    CURRENT_TIMESTAMP as analysis_timestamp,
    
    -- Current state
    target_satellite_count as "TARGET: Satellites Goal",
    current_active_satellites as "CURRENT: Active Satellites",
    satellites_still_needed as "REMAINING: Satellites Needed",
    ROUND((current_active_satellites * 100.0 / target_satellite_count)::numeric, 1) as "PROGRESS: Percent Complete",
    
    -- Historical context
    total_starlink_launches as "HISTORY: Total Starlink Launches",
    successful_starlink_launches as "HISTORY: Successful Launches",
    launch_success_rate_pct as "HISTORY: Success Rate %",
    ROUND(avg_satellites_per_successful_launch::numeric, 1) as "HISTORY: Avg Satellites per Launch",
    
    -- Recent performance indicators
    launches_last_24_months as "RECENT: Launches (24 months)",
    ROUND(avg_launches_per_month::numeric, 2) as "RECENT: Launches per Month",
    ROUND(avg_launches_per_year::numeric, 1) as "RECENT: Launches per Year",
    ROUND(recent_6mo_monthly_rate::numeric, 2) as "RECENT: Launches per Month (6mo trend)",
    launch_frequency_trend as "TREND: Launch Frequency",
    
    -- 🎯 BUSINESS ANSWERS - Multiple Scenarios
    conservative_launches_needed as "🎯 CONSERVATIVE: Launches Needed",
    ROUND(conservative_months_needed::numeric, 1) as "🎯 CONSERVATIVE: Months to 42K",
    CASE 
        WHEN conservative_months_needed IS NOT NULL THEN
            (CURRENT_DATE + INTERVAL '1 month' * conservative_months_needed)::date
        ELSE NULL
    END as "🎯 CONSERVATIVE: Est. Completion Date",
    
    current_pace_launches_needed as "🎯 CURRENT PACE: Launches Needed",
    ROUND(current_pace_months_needed::numeric, 1) as "🎯 CURRENT PACE: Months to 42K",
    CASE 
        WHEN current_pace_months_needed IS NOT NULL THEN
            (CURRENT_DATE + INTERVAL '1 month' * current_pace_months_needed)::date
        ELSE NULL
    END as "🎯 CURRENT PACE: Est. Completion Date",
    
    optimistic_launches_needed as "🎯 OPTIMISTIC: Launches Needed",
    CASE 
        WHEN GREATEST(COALESCE(recent_6mo_monthly_rate, 0), COALESCE(avg_launches_per_month, 0)) > 0 AND optimistic_launches_needed > 0 THEN
            ROUND((optimistic_launches_needed / GREATEST(recent_6mo_monthly_rate, avg_launches_per_month))::numeric, 1)
        ELSE NULL
    END as "🎯 OPTIMISTIC: Months to 42K",
    
    -- Confidence and risk assessment
    CASE 
        WHEN launches_last_24_months >= 24 AND recent_successful_launches >= 6 THEN 'High Confidence'
        WHEN launches_last_24_months >= 12 AND recent_successful_launches >= 3 THEN 'Medium Confidence'
        WHEN launches_last_24_months >= 6 THEN 'Low Confidence'
        ELSE 'Insufficient Data'
    END as "CONFIDENCE: Projection Reliability",
    
    -- Risk factors
    CASE 
        WHEN hours_since_last_update > 48 THEN 'Data Staleness Risk'
        WHEN launch_success_rate_pct < 90 THEN 'Launch Reliability Risk'
        WHEN launch_frequency_trend = 'Decelerating' THEN 'Pace Deceleration Risk'
        WHEN satellites_still_needed > current_active_satellites THEN 'Scale Challenge Risk'
        ELSE 'Low Risk'
    END as "RISK: Primary Risk Factor",
    
    -- Performance metrics for analysts
    ROUND(COALESCE(actual_satellites_per_successful_launch, 0)::numeric, 1) as "METRIC: Actual Sats per Success Launch",
    CASE 
        WHEN recent_successful_launches > 0 THEN
            ROUND((recent_total_launches::numeric / recent_successful_launches)::numeric, 2)
        ELSE NULL
    END as "METRIC: Recent Success Rate",
    hours_since_last_update as "METRIC: Data Age (hours)",
    
    -- Key assumptions for transparency
    'Based on recent 24-month performance data' as "ASSUMPTION: Time Period",
    'Excludes satellite deorbiting/failures' as "ASSUMPTION: Satellite Longevity",
    'Assumes current technology and regulations' as "ASSUMPTION: External Factors"

FROM projection_scenarios

-- Add model documentation
{{ config(
    post_hook="COMMENT ON TABLE {{ this }} IS 'Business intelligence table answering: When will SpaceX reach 42,000 Starlink satellites and how many launches will it take?'"
) }}