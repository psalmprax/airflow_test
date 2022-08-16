-- We consider the payment from 01-10-2021. The birthdate field was added at 31-08-2021

WITH offer AS (

    SELECT 
        owo.id,
        owo.trade_id,
        owo.calculated_payment_date,
        NOT owo.date_paid_mail_sent IS NULL AS is_offer_paid_mail_sent,
	    owo.number_of_stocks ,
	    owo.offer_state,
	    owo.device_id,
		owo.paid_amount
    FROM {{ ref('obj_wkfs_offer') }} owo 
    WHERE created_at >= '2021-09-01'
)

,offer_stock AS (
    SELECT 
        owo.id,
        owo.trade_id,
        owo.calculated_payment_date ,
        owo.is_offer_paid_mail_sent,
	    owo.number_of_stocks ,
	    owo.offer_state,
	    owo.device_id,
		owo.paid_amount,
        ows.stock_out_channel
	FROM  offer owo 
    LEFT JOIN {{ ref('obj_wkfs_stock') }} ows
      ON owo.id = ows.offer_id 
   
)
, filter_1 AS (
SELECT 
   id,
   trade_id,
   calculated_payment_date,
   is_offer_paid_mail_sent,
   number_of_stocks,
   offer_state,
   device_id,
   paid_amount,
   stock_out_channel
FROM offer_stock
WHERE offer_state NOT IN ('04 offene Klärung', '06 back to seller', '45 keine Auszahlung','53 ID Sperre','15 stock out')
      OR offer_state IS NULL
)
, filter_2 AS (
  SELECT 
   id,
   trade_id,
   calculated_payment_date,
   is_offer_paid_mail_sent,
   number_of_stocks,
   offer_state,
   device_id,
   paid_amount,
   stock_out_channel
FROM offer_stock
WHERE (offer_state = '15 stock out') AND (stock_out_channel != 'back2seller')

)

SELECT 
* FROM filter_1
UNION 
SELECT * FROM filter_2

