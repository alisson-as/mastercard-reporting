-- ===================================================================================
-- ETL Job: Bronze to Silver Transformation for Cards
-- Description: This script cleans, standardizes, and enriches the bronze cards
-- Data from the Bronze layer to create a well-structured Silver table.
-- Source Table: bronze.mastercard_reporting.cards
-- Target Table: silver.mastercard_reporting.cards
-- Granularity: One record per card_number and card
-- ===================================================================================

SELECT
  -- Clean and cast identifiers
   CAST(card_number AS BIGINT)  AS card_number

  -- Modifying model to 1:PIN, 2:CONTACTLESS, -1:WITOUT CORRESPONDENCE
  ,CASE 
    WHEN TRIM(UPPER(card_model)) = 'PIN' THEN 1    
    WHEN TRIM(UPPER(card_model)) = 'CONTACTLESS' THEN 2
    ELSE -1
  END AS card_model 
  ,CAST(TRIM(company_id)  AS VARCHAR(100))  AS company_id
  ,CAST(TRIM(employee_id) AS VARCHAR(100))  AS employee_id

  -- Modification valid_thru the last day of the month to facilitate the use of possible data manipulations and cross data
  ,LAST_DAY(to_date(CONCAT('01/', valid_thru), 'dd/MM/yy')) AS valid_thru

FROM bronze.mastercard_reporting.cards