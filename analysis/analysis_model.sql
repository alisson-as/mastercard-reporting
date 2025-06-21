-- Q1 (First Quarter): January 1, 2023 - March 31, 2023.
-- Q2 (Second Quarter): April 1, 2023 - June 30, 2023.
-- Q3 (Third Quarter): July 1, 2023 - September 30, 2023.

--Generating a list with all start dates of quarters
WITH sequence_days
AS(
-- Generating the days sequence of the start period at the end period recording at table cards_status
  SELECT explode( -- explode function to transform the array into a table
    sequence( -- sequence function to generate a list of dates between the start and end date
       (SELECT MIN(started_at) FROM silver.mastercard_reporting.cards_status)
      ,(SELECT MAX(COALESCE(ended_at, current_date())) FROM silver.mastercard_reporting.cards_status WHERE ended_at <> '9999-12-31')
      ,INTERVAL 1 DAY
    )
  ) AS exploded_day
),

quarters AS (
  SELECT DISTINCT
    DATE_TRUNC('QUARTER', exploded_day) AS quarter_start_date
  FROM sequence_days
)

SELECT
   CAST(q.quarter_start_date AS DATE) AS quarter_start_date
  ,CONCAT('Q', date_part('quarter', q.quarter_start_date), '/', date_part('year', q.quarter_start_date)) AS quarter
  ,CASE 
    WHEN c.card_model = 1 THEN 'PIN' 
    WHEN c.card_model = 2 THEN 'CONTACTLESS'
  END AS card_model
  ,COUNT(DISTINCT c.card_number) AS qty_cards_end_quarter

FROM quarters AS q
LEFT JOIN silver.mastercard_reporting.cards_status AS c
  ON c.started_at <= DATE_ADD(ADD_MONTHS(quarter_start_date, 3), -1) -- Considering all cards with started_at until the last day of quarter analysed
  AND (c.is_status_currently_active = 1 OR c.ended_at > DATE_ADD(ADD_MONTHS(quarter_start_date, 3), -1)) -- Getting all cards that were active (ended_at = '9999-12-31') or that were active at the start of the quarter analysed

WHERE 1=1
AND q.quarter_start_date >= '2023-01-01'
AND q.quarter_start_date < '2023-10-01'

GROUP BY
   q.quarter_start_date
  ,CONCAT('Q', date_part('quarter', q.quarter_start_date), '/', date_part('year', q.quarter_start_date))
  ,c.card_model

HAVING 
  COUNT(DISTINCT c.card_number) > 0