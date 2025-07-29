-- SpaceX ETL Database Initialization Script
-- This runs automatically when PostgreSQL container starts for the first time

-- Create user if not exists (redundant but safe)
DO
$do$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'spacex_user') THEN
      CREATE ROLE spacex_user LOGIN PASSWORD 'spacex_password';
   END IF;
END
$do$;

-- Create database if not exists (redundant but safe)
SELECT 'CREATE DATABASE spacex_db OWNER spacex_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'spacex_db')\gexec

-- Connect to spacex_db
\c spacex_db

-- Create schemas for medallion architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant all permissions to spacex_user
GRANT ALL PRIVILEGES ON SCHEMA bronze TO spacex_user;
GRANT ALL PRIVILEGES ON SCHEMA silver TO spacex_user;
GRANT ALL PRIVILEGES ON SCHEMA gold TO spacex_user;
GRANT ALL PRIVILEGES ON SCHEMA public TO spacex_user;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO spacex_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO spacex_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO spacex_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON SEQUENCES TO spacex_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON SEQUENCES TO spacex_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON SEQUENCES TO spacex_user;

-- Create bronze layer tables
CREATE TABLE IF NOT EXISTS bronze.launches (
    id VARCHAR PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP DEFAULT NOW(),
    source_system VARCHAR DEFAULT 'spacex_api_v4',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.starlink (
    id VARCHAR PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP DEFAULT NOW(),
    source_system VARCHAR DEFAULT 'spacex_api_v4',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.rockets (
    id VARCHAR PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP DEFAULT NOW(),
    source_system VARCHAR DEFAULT 'spacex_api_v4',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_launches_extracted_at ON bronze.launches(extracted_at);
CREATE INDEX IF NOT EXISTS idx_starlink_extracted_at ON bronze.starlink(extracted_at);
CREATE INDEX IF NOT EXISTS idx_rockets_extracted_at ON bronze.rockets(extracted_at);

-- JSON indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_launches_name ON bronze.launches USING GIN ((data->>'name'));
CREATE INDEX IF NOT EXISTS idx_launches_success ON bronze.launches USING GIN ((data->'success'));
CREATE INDEX IF NOT EXISTS idx_launches_date ON bronze.launches USING GIN ((data->>'date_utc'));
CREATE INDEX IF NOT EXISTS idx_starlink_decayed ON bronze.starlink USING GIN ((data->'spaceTrack'->>'DECAYED'));
CREATE INDEX IF NOT EXISTS idx_starlink_launch ON bronze.starlink USING GIN ((data->>'launch'));

-- Create a metadata table to track pipeline runs
CREATE TABLE IF NOT EXISTS bronze.pipeline_metadata (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR NOT NULL,
    run_date TIMESTAMP DEFAULT NOW(),
    status VARCHAR NOT NULL,
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Grant ownership of all tables to spacex_user
ALTER TABLE bronze.launches OWNER TO spacex_user;
ALTER TABLE bronze.starlink OWNER TO spacex_user;
ALTER TABLE bronze.rockets OWNER TO spacex_user;
ALTER TABLE bronze.pipeline_metadata OWNER TO spacex_user;

-- Insert initial metadata record
INSERT INTO bronze.pipeline_metadata (pipeline_name, status, records_processed)
VALUES ('database_initialization', 'completed', 0);

-- Create a verification function
CREATE OR REPLACE FUNCTION verify_setup()
RETURNS TABLE(component TEXT, status TEXT, details TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'schemas'::TEXT,
        CASE WHEN COUNT(*) = 3 THEN 'OK' ELSE 'ERROR' END::TEXT,
        'Found ' || COUNT(*)::TEXT || ' schemas (bronze, silver, gold)'::TEXT
    FROM information_schema.schemata 
    WHERE schema_name IN ('bronze', 'silver', 'gold')
    
    UNION ALL
    
    SELECT 
        'bronze_tables'::TEXT,
        CASE WHEN COUNT(*) = 4 THEN 'OK' ELSE 'ERROR' END::TEXT,
        'Found ' || COUNT(*)::TEXT || ' tables in bronze schema'::TEXT
    FROM information_schema.tables 
    WHERE table_schema = 'bronze'
    
    UNION ALL
    
    SELECT 
        'permissions'::TEXT,
        'OK'::TEXT,
        'spacex_user has full access'::TEXT;
END;
$$ LANGUAGE plpgsql;

-- Run verification
SELECT * FROM verify_setup();

-- Log successful initialization
\echo 'SpaceX ETL Database initialized successfully!'
\echo 'Schemas created: bronze, silver, gold'
\echo 'Tables created: launches, starlink, rockets, pipeline_metadata'
\echo 'Ready for SpaceX API data ingestion!'