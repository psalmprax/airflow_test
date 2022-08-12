WITH latest_trades AS (
   SELECT 
        owt.transaction_id  
        , owt.user_id 
        , owt.created_at
    FROM {{ ref('obj_wkfs_trade') }} owt
    WHERE extract(year FROM created_at) >= extract(year FROM current_timestamp) - 5
    
)

,latest_user_trades AS (
SELECT 
     u.id AS user_id
   , u.firstname
   , u.lastname
   , u.street
   , u.email
   , lt.transaction_id
   , lt.created_at
FROM latest_trades AS lt
JOIN {{ ref('obj_wkfs_user') }} AS u
  ON lt.user_id  = u.id
ORDER BY email,created_at
   
)

,user_number_trades AS 
(
SELECT 
    user_id
  , transaction_id
  , email
  , firstname
  , lastname
  , street
  , created_at
  , count(transaction_id) OVER(PARTITION BY email) AS number_trades
FROM latest_user_trades
--WINDOW w AS (PARTITION BY email)
--  get only the timestamp of the first trade for each customer
)

,first_user_number_trades AS 
(
SELECT 
* 
FROM 
(
    SELECT 
        user_id
        , transaction_id
        , email
        , firstname
        , lastname
        , street
        , created_at
        , number_trades
        , rank() OVER (PARTITION BY email ORDER BY created_at) AS rk
    FROM user_number_trades
--    WINDOW w AS (PARTITION BY email ORDER BY created_at)
) AS funt
WHERE rk = 1
)

,latest_user_offer AS (
SELECT 
    -- last user trades
  owo.trade_id
  , unt.user_id
  , unt.email
  , unt.firstname
  , unt.lastname
  , unt.street
  -- obj_wkfs_offer
  , owo.id AS offer_id
  , owo.device_id AS ta
  , owo.created_at
  , owo.device 
  , owo.category 
  , owo.model
  , owo.offer_state
  , owo.promo_value
  , owo.retoure_description
  , owo.retoure_reason_type
  , owo.accepted_amount 
  , owo.converted_amount 
  , unt.number_trades
FROM user_number_trades  AS unt 
JOIN {{ ref('obj_wkfs_offer') }} AS owo 
  ON unt.transaction_id = owo.trade_id 
)

,user_offer_retoure AS 
(
SELECT
    offer_id
  , user_id
  , trade_id 
  , email
  , firstname
  , lastname
  , street
  , created_at
  , extract(year FROM created_at) AS year_created 
  , ta
  , device
  , category
  , model
  , promo_value
  , accepted_amount
  , converted_amount
  , count(retoure_reason_type) OVER (PARTITION BY email) AS number_retoure
  , sum(accepted_amount) OVER (PARTITION BY email,extract(year FROM created_at)
    rows between unbounded preceding and unbounded following) AS total_accepted_amount
  , sum(converted_amount) OVER (PARTITION BY email,extract(year FROM created_at)
    rows between unbounded preceding and unbounded following) AS total_converted_amount
  , coalesce(sum(accepted_amount) over (PARTITION BY email) / number_trades , 0.0)  AS avg_accepted_amount
  , offer_state
  , sum(CASE WHEN offer_state = '06 back to seller' THEN 1 ELSE 0 END) OVER (PARTITION BY email) AS count_back_to_seller
  , retoure_reason_type
  , retoure_description
FROM latest_user_offer
--WINDOW  w_email AS (PARTITION BY email),
--        w_email_year AS (PARTITION BY email,extract(year FROM created_at))
-- rows between unbounded preceding and unbounded following
)

, offer_clarifications as 
(

SELECT
  *
  , sum(CASE WHEN is_clarification_questionnaire = true THEN 1 ELSE 0 END) OVER (PARTITION BY email) AS count_clarification_questionnaire
  , sum(CASE WHEN is_clarification_technical = true THEN 1 ELSE 0 END) OVER (PARTITION BY email) AS count_clarification_technical
  , sum(CASE WHEN is_clarification_optical = true THEN 1 ELSE 0 END) OVER (PARTITION BY email) AS count_clarification_optical
  , sum(CASE WHEN is_clarification_general_data = true THEN 1 ELSE 0 END) OVER (PARTITION BY email) AS count_clarification_general_data
FROM
(
  SELECT 
	  uor.*
	  , ows.is_clarification_questionnaire
	  , ows.is_clarification_technical
	  , ows.is_clarification_optical
	  , ows.is_clarification_general_data
	FROM user_offer_retoure uor
	JOIN {{ ref('obj_wkfs_stock') }} ows 
	 ON uor.offer_id = ows.offer_id 
	) s
--WINDOW  w_email AS (PARTITION BY email)

)

SELECT
    oc.offer_id
    , funt.user_id
    , funt.email
    , funt.firstname
    , funt.lastname
    , funt.street
    , funt.number_trades
    , funt.created_at AS first_trade
    , oc.created_at
    , oc.year_created 
    , oc.total_accepted_amount
    , oc.total_converted_amount
    , oc.avg_accepted_amount
    , oc.ta
    , oc.device 
    , oc.category
    , oc.model
    , oc.promo_value
    , oc.number_retoure
    , oc.retoure_reason_type
    , oc.retoure_description
    , oc.count_back_to_seller
    , oc.count_clarification_questionnaire
    , oc.count_clarification_technical
    , oc.count_clarification_optical
    , oc.count_clarification_general_data
       
FROM first_user_number_trades AS funt
JOIN offer_clarifications AS oc
  ON funt.email = oc.email
  