SELECT
   valid_thru
  ,COUNT(DISTINCT card_number) AS qty_cards_per_valid_thru

FROM silver.mastercard_reporting.cards

WHERE 1=1
AND valid_thru >= '2023-10-31'

AND card_model = 1

GROUP BY
  valid_thru