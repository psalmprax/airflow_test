WITH article_history AS 
(
SELECT distinct
  id
  , article_number
  , rsp
  , created_at::date AS date_from
  , lead(created_at::date,1) OVER (PARTITION BY article_number ORDER BY created_at::date) AS date_to
  , lead(rsp,1) OVER (PARTITION BY article_number ORDER BY created_at::date) AS next_rsp

FROM  {{ source('wkfs', 'article_numbers_rsp_history') }} anrh

--WINDOW w AS (PARTITION BY article_number ORDER BY created_at::date)
)
, offer_stock AS 
(
SELECT 
    owo.id
   , owo.article_number
   , ows.stock_out_price
   , owo.date_paid 
   
FROM  {{ ref('obj_wkfs_offer') }} owo
JOIN  {{ ref('obj_wkfs_stock') }} ows
   ON owo.id = ows.offer_id 
WHERE owo.created_at::date >= (SELECT min(date_from) FROM article_history)
   AND owo.state = 'paid' 
   AND ows.stock_out IS NOT NULL
  

)
, stock_price_partition AS 
(
 SELECT distinct
   * 
   , lead(date_paid::date,1) OVER (PARTITION BY article_number ORDER BY date_paid::date) AS next_paid
   , lead(stock_out_price,1) OVER (PARTITION BY article_number ORDER BY date_paid::date) AS next_stock_out_price
 FROM offer_stock
-- WINDOW w AS (PARTITION BY article_number ORDER BY date_paid::date)
)
, offer_stock_article_history AS (
SELECT 
    os.id
  , os.article_number
  , os.date_paid::date
  , os.next_paid::date
  , ah.date_from
  , ah.date_to
  , os.stock_out_price
  , os.next_stock_out_price
  , ah.rsp
  , ah.next_rsp
FROM article_history AS ah
JOIN stock_price_partition AS os
  ON os.article_number = ah.article_number

)
, selected_stock_price AS
(

/** 
 In many cases we have the rsp change in certain date range, but also the stock_out_price
 values have there own ranges
 In order to not have for every rsp in a certain date ranges repeated stock out prices
 we select the correct stock out price if the date_from of the article_rsp history is 
 in one of the ranges of date_paid from stock
 
**/
SELECT
* 
FROM 
	(SELECT 
		* 
		, CASE 
		     WHEN date_from < min(date_paid) OVER (PARTITION BY article_number,date_from) THEN min(date_paid) OVER (PARTITION BY article_number,date_from)
		     WHEN date_from >= date_paid AND date_from < next_paid THEN date_paid
		     WHEN date_from >= max(date_paid) OVER (PARTITION BY article_number,date_from) THEN max(date_paid) OVER (PARTITION BY article_number,date_from)
		     END AS sel_date
		
		FROM offer_stock_article_history
--		WINDOW w AS (PARTITION BY article_number,date_from)
	) s
WHERE sel_date = date_paid
)

SELECT 
   id
   , article_number
   , date_from
   , date_to
   , stock_out_price
   , rsp
   , next_rsp
FROM selected_stock_price
ORDER BY article_number,date_from 
