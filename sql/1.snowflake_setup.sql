-- ============================================================================
-- Copyright 2025 Snowflake Inc.
-- SPDX-License-Identifier: Apache-2.0
-- Licensed under the Apache License, Version 2.0 (the "License");
-- You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
-- ============================================================================
--
-- OpenFlow PostgreSQL CDC Quickstart - Snowflake Setup
--
-- This script sets up all required Snowflake objects for the healthcare CDC demo
-- Run this BEFORE configuring the OpenFlow connector
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Step 1: Create Role and Database
-- ----------------------------------------------------------------------------

-- Create runtime role (reuse if coming from SPCS quickstart)
CREATE ROLE IF NOT EXISTS QUICKSTART_ROLE;

-- Create database for healthcare data
CREATE DATABASE IF NOT EXISTS QUICKSTART_PGCDC_DB;

-- Create warehouse for data processing
CREATE WAREHOUSE IF NOT EXISTS QUICKSTART_PGCDC_WH
  WAREHOUSE_SIZE = MEDIUM
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

-- Grant privileges to runtime role
GRANT OWNERSHIP ON DATABASE QUICKSTART_PGCDC_DB TO ROLE QUICKSTART_ROLE;
GRANT OWNERSHIP ON SCHEMA QUICKSTART_PGCDC_DB.PUBLIC TO ROLE QUICKSTART_ROLE;
GRANT USAGE ON WAREHOUSE QUICKSTART_PGCDC_WH TO ROLE QUICKSTART_ROLE;

-- Grant runtime role to OpenFlow admin
GRANT ROLE QUICKSTART_ROLE TO ROLE OPENFLOW_ADMIN;

-- Step 2: Create Schema and Network Rules
-- ----------------------------------------------------------------------------

USE ROLE QUICKSTART_ROLE;
USE DATABASE QUICKSTART_PGCDC_DB;

-- Create schema for network rules
CREATE SCHEMA IF NOT EXISTS QUICKSTART_PGCDC_DB.NETWORKS;
-- Note: Do NOT create the healthcare data schema here. Openflow will
-- automatically create the source schema (e.g., "healthcare") during the
-- initial snapshot load in Snowflake.

-- Create stage for Snowflake Intelligence semantic models
USE SCHEMA PUBLIC;
CREATE STAGE IF NOT EXISTS semantic_models
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Stage for Snowflake Intelligence semantic models';

-- Grant usage to ACCOUNTADMIN for Snowflake Intelligence agent creation
GRANT READ ON STAGE semantic_models TO ROLE ACCOUNTADMIN;

-- Step 3: Create Network Rules
-- ----------------------------------------------------------------------------
-- IMPORTANT: Replace with your PostgreSQL endpoint
-- 
-- This quickstart uses GCP Cloud SQL, but you can use any PostgreSQL service:
-- - GCP Cloud SQL:        '34.123.45.67:5432' (public IP)
-- - AWS RDS:              'mydb.abc123.us-east-1.rds.amazonaws.com:5432'
-- - Azure Database:       'myserver.postgres.database.azure.com:5432'
-- - Self-hosted:          'your-postgres-server.com:5432'
-- - Local/On-premises:    '10.0.1.50:5432'
--
-- Note: Ensure network connectivity and firewall rules allow Snowflake access

CREATE OR REPLACE NETWORK RULE QUICKSTART_PGCDC_DB.NETWORKS.postgres_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('YOUR-POSTGRES-HOST:5432'); -- Replace with your PostgreSQL endpoint

-- Step 4: Create External Access Integration
-- ----------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION quickstart_pgcdc_access
  ALLOWED_NETWORK_RULES = (
    QUICKSTART_PGCDC_DB.NETWORKS.postgres_network_rule
  )
  ENABLED = TRUE
  COMMENT = 'OpenFlow SPCS runtime access for PostgreSQL CDC';

-- Grant usage to runtime role
GRANT USAGE ON INTEGRATION quickstart_pgcdc_access TO ROLE QUICKSTART_ROLE;

-- Step 5: Setup Snowflake Intelligence
-- ----------------------------------------------------------------------------

-- Create database for Snowflake Intelligence
CREATE DATABASE IF NOT EXISTS snowflake_intelligence;
GRANT USAGE ON DATABASE snowflake_intelligence TO ROLE PUBLIC;

-- Create agents schema
CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.agents;
GRANT USAGE ON SCHEMA snowflake_intelligence.agents TO ROLE PUBLIC;

-- Grant agent creation privileges to quickstart role
GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE QUICKSTART_ROLE;

-- Step 6: Verify Setup
-- ----------------------------------------------------------------------------

USE ROLE QUICKSTART_ROLE;

-- Verify role and grants
SHOW ROLES LIKE 'QUICKSTART_ROLE';
SHOW GRANTS TO ROLE QUICKSTART_ROLE;

-- Verify database and schemas
SHOW SCHEMAS IN DATABASE QUICKSTART_PGCDC_DB;

-- Verify integration
SHOW INTEGRATIONS LIKE 'quickstart_pgcdc_access';
DESC INTEGRATION quickstart_pgcdc_access;

-- ============================================================================
-- Setup Complete!
-- ============================================================================
-- Next Steps:
-- 1. Update the postgres_network_rule with your PostgreSQL endpoint
-- 2. Run 1.init_healthcare.sql on your PostgreSQL database
-- 3. Configure OpenFlow CDC connector (tables will be auto-created during snapshot)
-- 4. Run 2.verify_snapshot.sql to verify the snapshot load
-- ============================================================================
