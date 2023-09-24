-- Create ETL User
CREATE USER etl WITH PASSWORD 'test_pwd';
-- Grant Connect
GRANT CONNECT ON DATABASE "AdventureWorks" TO etl;
-- Grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl;