-- =============================================================
-- Create Database and Schemas
-- =============================================================
-- Script Purpose:
--   Creates a new database named 'datawarehouse'.
--   If it already exists, it will be dropped and recreated.
--   Also creates three schemas: bronze, silver, and gold.
-- =============================================================

-- Step 1: Drop the database if it already exists
DROP DATABASE IF EXISTS datawarehouse;

-- Step 2: Create the new database
CREATE DATABASE datawarehouse;

-- Step 3: Connect to the new database
-- (In pgAdmin, open a new query window on 'datawarehouse' before running the next part)

-- Step 4: Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
