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
),

-- Union list of quarters with the list of cards that were active at the start of each quarter

active_beginning_quarter AS (
  SELECT
     CAST(q.quarter_start_date AS DATE) AS quarter_start_date
    ,COUNT(DISTINCT CASE WHEN c.card_status = 1 THEN c.card_number END) AS qty_cards_beginning_quarter_open
    ,COUNT(DISTINCT CASE WHEN c.card_status = 2 THEN c.card_number END) AS qty_cards_beginning_quarter_temp_blocked
    ,COUNT(DISTINCT c.card_number) AS qty_cards_beginning_quarter_total

  FROM quarters AS q
  LEFT JOIN silver.mastercard_reporting.cards_status AS c
    ON c.started_at < q.quarter_start_date -- Getting always the latest date previous quarter
    AND (c.is_status_currently_active = 1 OR c.ended_at >= q.quarter_start_date) -- Getting all cards that were active (ended_at = '9999-12-31') or that were active at the start of the quarter analysed

  GROUP BY
    q.quarter_start_date

  HAVING 
    COUNT(DISTINCT c.card_number) > 0
),

cards_open_beginning_quarter AS (
  SELECT
     DISTINCT
     CAST(q.quarter_start_date AS DATE) AS quarter_start_date
    ,c.card_number

  FROM quarters AS q
  LEFT JOIN silver.mastercard_reporting.cards_status AS c
    ON c.started_at < q.quarter_start_date -- Getting always the latest date previous quarter
    AND card_status = 1
),
-- Counting all cards that were obtained into the quarter
new_cards_obtained_into_quarter AS (
  SELECT
     CAST(q.quarter_start_date AS DATE) AS quarter_start_date
    ,COUNT(DISTINCT c.card_number) AS qty_cards_obtained_quarter

  FROM quarters AS q
  LEFT JOIN silver.mastercard_reporting.cards_status AS c
    ON c.started_at >= q.quarter_start_date
    AND c.started_at <= DATE_ADD(ADD_MONTHS(quarter_start_date, 3), -1)
    AND card_status = 1
  LEFT JOIN cards_open_beginning_quarter AS co
    ON c.card_number = co.card_number
    AND q.quarter_start_date = co.quarter_start_date

  WHERE 1=1
  AND co.card_number IS NULL

  GROUP BY
    q.quarter_start_date
),

cards_terminated_into_quarter AS (
  SELECT
     CAST(q.quarter_start_date AS DATE) AS quarter_start_date
    ,COUNT(DISTINCT c.card_number) AS qty_cards_terminated_quarter

  FROM quarters AS q
  LEFT JOIN silver.mastercard_reporting.cards_status AS c
      ON c.ended_at >= q.quarter_start_date
      AND c.ended_at <= DATE_ADD(ADD_MONTHS(quarter_start_date, 3), -1)
      AND c.card_status = 3

  GROUP BY
    q.quarter_start_date
),

active_end_quarter AS (
  SELECT
     CAST(q.quarter_start_date AS DATE) AS quarter_start_date
    ,COUNT(DISTINCT CASE WHEN c.card_status = 1 THEN c.card_number END) AS qty_cards_end_quarter_open
    ,COUNT(DISTINCT CASE WHEN c.card_status = 2 THEN c.card_number END) AS qty_cards_end_quarter_temp_blocked
    ,COUNT(DISTINCT c.card_number) AS qty_cards_end_quarter_total

  FROM quarters AS q
  LEFT JOIN silver.mastercard_reporting.cards_status AS c
    ON c.started_at <= DATE_ADD(ADD_MONTHS(quarter_start_date, 3), -1) -- Considering all cards with started_at until the last day of quarter analysed
    AND (c.is_status_currently_active = 1 OR c.ended_at > DATE_ADD(ADD_MONTHS(quarter_start_date, 3), -1)) -- Getting all cards that were active (ended_at = '9999-12-31') or that were active at the start of the quarter analysed

  GROUP BY
    q.quarter_start_date

  HAVING 
    COUNT(DISTINCT c.card_number) > 0
),

-- CTE to count the number of transactions per card and quarter
cards_yes_transactions_into_quarter AS (
  SELECT
     CAST(q.quarter_start_date AS DATE) AS quarter_start_date
    ,COUNT(DISTINCT ct.card_number) AS qty_cards_transactions_quarter

  FROM quarters AS q
  LEFT JOIN silver.mastercard_reporting.cards_transactions ct
    ON ct.transaction_date >= q.quarter_start_date
      AND ct.transaction_date <= DATE_ADD(ADD_MONTHS(quarter_start_date, 3), -1)
      
  GROUP BY
    CAST(q.quarter_start_date AS DATE)
)

-- Grouping and counting cards in each quarter
SELECT
   ab.quarter_start_date
  ,CONCAT('Q', date_part('quarter', ab.quarter_start_date), '/', date_part('year', ab.quarter_start_date)) AS quarter
  ,ab.qty_cards_beginning_quarter_open
  ,ab.qty_cards_beginning_quarter_temp_blocked
  ,ab.qty_cards_beginning_quarter_total
  ,ncoiq.qty_cards_obtained_quarter
  ,ct.qty_cards_terminated_quarter
  ,ae.qty_cards_end_quarter_open
  ,ae.qty_cards_end_quarter_temp_blocked
  ,ae.qty_cards_end_quarter_total
  ,cyt.qty_cards_transactions_quarter
  
FROM active_beginning_quarter AS ab
LEFT JOIN new_cards_obtained_into_quarter AS ncoiq
  ON ab.quarter_start_date = ncoiq.quarter_start_date
LEFT JOIN cards_terminated_into_quarter AS ct
  ON ab.quarter_start_date = ct.quarter_start_date
LEFT JOIN active_end_quarter AS ae
  ON ab.quarter_start_date = ae.quarter_start_date
LEFT JOIN cards_yes_transactions_into_quarter AS cyt
  ON ab.quarter_start_date = cyt.quarter_start_date

WHERE 1=1
AND ab.quarter_start_date >= '2023-01-01'
AND ab.quarter_start_date < '2023-10-01'

ORDER BY
  ab.quarter_start_date