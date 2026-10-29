-- Automatically initialize separate logical databases for each bounded context
SELECT 'CREATE DATABASE purchases_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'purchases_db')\gexec

SELECT 'CREATE DATABASE loyalty_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'loyalty_db')\gexec

SELECT 'CREATE DATABASE analytics_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'analytics_db')\gexec
