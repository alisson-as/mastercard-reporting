-- ===================================================================================
-- ETL Job: Bronze to Silver Transformation for Cards Status
-- Description: This script cleans, standardizes, and enriches the bronze cards_status
-- Data from the Bronze layer to create a well-structured Silver table.
-- Source Table: bronze.mastercard_reporting.cards_status
-- Target Table: silver.mastercard_reporting.cards_status
-- Granularity: One record per card_number and card_status
-- ===================================================================================

-- Create a variable with current date from transactions table
-- I decided to use this date, because it don't have database with recent data of the contrutions this query
WITH current_date_trans AS (
  SELECT 
    CAST(MAX(transaction_date) AS DATE) AS current_date_trans
  FROM bronze.mastercard_reporting.cards_transactions
),

table_standardized AS (
  SELECT
    -- Clean and cast identifiers
     CAST(card_number AS BIGINT)  AS card_number
    
    -- Standardize string fields to uppercase and trim whitespace

    -- Modifying model to 1:PIN, 2:CONTACTLESS, -1:WITOUT CORRESPONDENCE
    ,CAST(
      CASE 
        WHEN TRIM(UPPER(card_model)) = 'PIN' THEN 1    
        WHEN TRIM(UPPER(card_model)) = 'CONTACTLESS' THEN 2
        ELSE -1
      END
    AS INT) AS card_model
    -- Modifying type to 1:PLASTIC and -1:WITOUT CORRESPONDENCE
    ,CAST(
        CASE 
        WHEN TRIM(UPPER(card_type)) = 'PLASTIC' THEN 1
        ELSE -1
      END
    AS INT) AS card_type
    -- Modifying status to 1:OPEN, 2:TEMPORARILY_BLOCKED, 3:PERMANENTLY_TERMINATED and -1:WITOUT CORRESPONDENCE
    ,CAST(
      CASE 
        WHEN TRIM(UPPER(card_status)) = 'OPEN' THEN 1    
        WHEN TRIM(UPPER(card_status)) = 'TEMPORARILY_BLOCKED' THEN 2
        WHEN TRIM(UPPER(card_status)) = 'PERMANENTLY_TERMINATED' THEN 3
        ELSE -1
      END
    AS INT) AS card_status
    -- Cast string dates to proper DATE types
    ,CAST(started_at AS DATE)      AS started_at

    -- Create a final, clean end date. Using a far-future date for NULLs simplifies
    -- When card_status equals PERMANENTLY_TERMINATED, it'll use the start date 
    -- Because, the understanding that status=PERMANENTLY_TERMINATED isn't possible to return to status=OPEN and remain with the same card number
    ,CAST(
      CASE 
        WHEN TRIM(UPPER(card_status)) = 'PERMANENTLY_TERMINATED' AND ended_at = 'null' THEN started_at
        ELSE COALESCE(ended_at, '9999-12-31') 
      END
    AS DATE) AS ended_at

  FROM bronze.mastercard_reporting.cards_status
),
-- Enrich the data with calculated fields and flags.
table_enriched AS (
  SELECT
     -- Columns from CTE: table_standardized
     tbs.card_number
    ,tbs.card_model
    ,tbs.card_type
    ,tbs.card_status
    ,tbs.started_at
    ,tbs.ended_at

    -- Flag to identify the most recent record for each card (based on start date)
    -- This is crucial for finding the "current" status.
    ,CAST(
      CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY tbs.card_number ORDER BY tbs.started_at DESC, tbs.card_status DESC) = 1 THEN 1
        ELSE 0
      END
    AS INT) AS is_latest_record
    -- A more business-friendly flag to identify cards that are currently active.
    -- A record is active if its end date is NULL and it's the latest known record.
    ,CAST(
      CASE 
        WHEN (tbs.ended_at = '9999-12-31' AND ROW_NUMBER() OVER (PARTITION BY tbs.card_number ORDER BY tbs.started_at DESC, tbs.card_status DESC) = 1) THEN 1
        ELSE 0
      END
    AS INT) AS is_status_currently_active
    -- Calculate the duration of each status in days. For active records,
    -- this calculates the duration up to the current date.
    ,CAST(
      DATEDIFF(day, tbs.started_at, COALESCE(tbs.ended_at, (SELECT MAX(current_date_trans) FROM current_date_trans)))
    AS INT) AS status_duration_days

  FROM table_standardized AS tbs
)
-- Final SELECT to shape the silver table
SELECT
   hash(CONCAT(card_number, card_status)) AS card_status_id -- Create a unique key
  ,card_number
  ,card_model
  ,card_type
  ,card_status
  ,started_at
  ,ended_at
  
  -- The new enriched fields that provide the most value
  ,is_latest_record
  ,is_status_currently_active
  ,status_duration_days

FROM table_enriched

-- WHERE 1=1
-- AND card_number = 114632520

ORDER BY
   card_number
  ,started_at