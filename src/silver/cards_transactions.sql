-- ===================================================================================
-- ETL Job: Bronze to Silver Transformation for Cards Transations
-- Description: This script cleans, standardizes, and enriches the bronze cards
-- Data from the Bronze layer to create a well-structured Silver table.
-- Source Table: bronze.mastercard_reporting.cards_transactions
-- Target Table: silver.mastercard_reporting.cards_transactions
-- Granularity: One record per card_number and cards_transactions
-- ===================================================================================

SELECT
  -- Clean and cast identifiers
   CAST(card_number AS BIGINT)  AS card_number
  ,CAST(TRIM(transaction_id) AS VARCHAR(100)) AS transaction_id
  ,CAST(transaction_date AS TIMESTAMP_NTZ) AS transaction_date
  ,CAST(amount AS FLOAT) AS amount

FROM bronze.mastercard_reporting.cards_transactions --TABLESAMPLE (0.01 PERCENT)