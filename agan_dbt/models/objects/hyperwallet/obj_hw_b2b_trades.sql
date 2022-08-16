WITH b2b_trades AS (
   
SELECT 
    id AS trade_id,
	user_id,
	payment_id,
	application_id,
	landingpage_id,
    company_name,
	NOT deleted = 0 AS is_trade_deleted
FROM {{ ref('obj_wkfs_trade') }} owt
WHERE (owt.application_id IN (1,40,43,19,60)) AND (owt.landingpage_id IN (394,754))

) 

SELECT 
    trade_id,
    user_id,
    payment_id,
    application_id,
    landingpage_id,
	company_name,
    is_trade_deleted
FROM  b2b_trades
WHERE is_trade_deleted = false

