#!/bin/bash
# Automated SpaceX Pipeline Setup Script

echo "🚀 Starting SpaceX ETL Pipeline Setup..."

# Stop any existing containers
echo "Stopping existing containers..."
astro dev stop

# Clean up old containers and volumes (optional)
echo "Cleaning up old SpaceX containers..."
docker stop spacex_postgres 2>/dev/null || true
docker rm spacex_postgres 2>/dev/null || true
docker volume rm spacex_postgres_data 2>/dev/null || true

# Start the pipeline
echo "Starting Astro development environment..."
astro dev start

# Wait for containers to be healthy
echo "Waiting for containers to be ready..."
sleep 30

# Verify setup
echo "Verifying database setup..."
docker exec spacex_postgres psql -U spacex_user -d spacex_db -c "SELECT * FROM verify_setup();"

# Test connection from Airflow
echo "Testing Airflow to SpaceX database connection..."
docker exec astro_webserver_1 python -c "
import psycopg2
try:
    conn = psycopg2.connect(host='spacex-postgres', user='spacex_user', password='spacex_password', database='spacex_db')
    print('✅ Airflow can connect to SpaceX database!')
    conn.close()
except Exception as e:
    print(f'❌ Connection failed: {e}')
"

echo "🎉 SpaceX ETL Pipeline setup complete!"
echo "📊 Airflow UI: http://localhost:8080 (admin/admin)"
echo "🗄️ SpaceX Database: localhost:5433 (spacex_user/spacex_password)"