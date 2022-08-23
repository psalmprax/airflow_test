{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='insert_overwrite',
        merge_update_columns=['id','created_at']
    )
}}
WITH business_channel_histories AS (
  -- only used in the next CTE
  SELECT
    offer_id
    , business_channel_id
    , ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY created_at DESC) AS rn
  FROM {{ ref('obj_wkfs.offer_business_channel_history_cleaned') }}
  ORDER BY created_at ASC
)
, original_condition AS (
  SELECT
    bch.offer_id, 
    bc.outlet_name as business_channel,
    du.usage AS condition_original
  FROM business_channel_histories AS bch
    LEFT JOIN {{ ref('obj_wkfs.business_channel_cleaned') }} AS bc
      ON bc.id = bch.business_channel_id
    LEFT JOIN {{ ref('obj_wkfs.calculation_cleaned') }} AS calc
      ON calc.id = bc.calculation_id
    LEFT JOIN {{ ref('obj_wkfs.device_usage_cleaned') }} AS du
      ON du.id = calc.usage_id
  WHERE bch.rn = 1
)
, refurbishments AS (
  SELECT
    offer_id
    , LISTAGG(part || ' (' || costs::TEXT || '€)', ';'::TEXT) AS refurbishments
    , SUM(costs) AS cost_refurbishments
  FROM {{ ref('obj_wkfs.offer_refurbishment_cleaned') }}
  GROUP BY 1
)
, retoures AS (
--  SELECT DISTINCT
--    stock.offer_id
--    , FIRST_VALUE(ret.description) OVER w AS retoure_description
--    , FIRST_VALUE(ret.retoure_reason_text) OVER w AS retoure_reason_detail
--    , FIRST_VALUE(ret.retoure_reason_id) OVER w AS retoure_reason_id
--    , FIRST_VALUE(ret.warranty) OVER w AS retoure_warranty
--  FROM {{ ref('obj_wkfs.stock_cleaned') }} AS stock
--  LEFT JOIN {{ ref('obj_wkfs.retoure_cleaned') }} AS ret
--    ON ret.stock_out_id = stock.id
--  WHERE NOT ret.stock_out_id IS NULL
--  WINDOW w AS (PARTITION BY stock.offer_id ORDER BY ret.id DESC)
  SELECT DISTINCT
    stock.offer_id
    , FIRST_VALUE(ret.description) OVER (PARTITION BY stock.offer_id ORDER BY ret.id DESC
      rows between unbounded preceding and unbounded following ) AS retoure_description
    , FIRST_VALUE(ret.retoure_reason_text) OVER (PARTITION BY stock.offer_id ORDER BY ret.id DESC
      rows between unbounded preceding and unbounded following ) AS retoure_reason_detail
    , FIRST_VALUE(ret.retoure_reason_id) OVER (PARTITION BY stock.offer_id ORDER BY ret.id DESC
      rows between unbounded preceding and unbounded following ) AS retoure_reason_id
    , FIRST_VALUE(ret.warranty) OVER (PARTITION BY stock.offer_id ORDER BY ret.id DESC
      rows between unbounded preceding and unbounded following ) AS retoure_warranty
  FROM {{ ref('obj_wkfs.stock_cleaned') }} AS stock
  LEFT JOIN {{ ref('obj_wkfs.retoure_cleaned') }} AS ret
    ON ret.stock_out_id = stock.id
  WHERE NOT ret.stock_out_id IS NULL
)
, first_last_stock_in AS (
  SELECT
    offer_id
    , MIN(stock_in) AS first_stock_in
    , MAX(stock_in) AS last_stock_in
    , MIN(stock_out) AS first_stock_out
    , MAX(stock_out) AS last_stock_out
    , MIN(clarification_start_at) AS first_clarification_start
    , MIN(clarification_end_at) AS first_clarification_end
  FROM {{ ref('obj_wkfs_stock') }}
  GROUP BY 1
)
, stock_out_channel_status AS (
  SELECT
   offer_id,
   id AS first_stock_id,
   status AS first_clarification_accepted,
   stock_out_channel AS first_stock_out_channel,
   ROW_NUMBER() OVER (PARTITION BY ows.offer_id ORDER BY stock_in) AS rn
	   
  FROM {{ ref('obj_wkfs_stock') }} AS ows 
--  WINDOW w AS (PARTITION BY ows.offer_id ORDER BY stock_in)
)
, first_stock_out_channel_status AS (
  SELECT 
    offer_id,
    first_stock_id,
    first_clarification_accepted,
    first_stock_out_channel
  FROM stock_out_channel_status
  WHERE rn = 1
)

SELECT distinct
  o.id
  {{ select_all_except(
    'wkfs',
    'offer',
    [
      'id'
      , 'accepted_amount'
      , 'converted_amount'
      , 'amount'
      , 'condition'
      , 'original_amount'
      , 'device_id'
      , "dag"
      , "data_interval_start"
      , "data_interval_end"
      , "execution_date"
      , "next_execution_date"
      , "prev_data_interval_start_success"
      , "prev_data_interval_end_success"
      , "prev_execution_date"
      , "prev_start_date_success"
      , "prev_execution_date_success"
    ],
    False,
    'o')
  }}
  , COALESCE(
    NULLIF(o.converted_amount, 0)
    , NULLIF(o.accepted_amount, 0)
    , o.original_amount
  ) AS base_price
  , dc.id AS condition_id
  , dc.code AS condition_code
  , dc.title AS condition_title
  , o.condition AS condition_user
  , refs.refurbishments
  , refs.cost_refurbishments
  , LPAD(o.id::TEXT, 6, '0') AS offer
  , ROUND(o.accepted_amount + COALESCE(txo.tax_amount, 0), 2) AS amount
  , ROUND(o.accepted_amount + COALESCE(txo.tax_amount, 0), 2)
    * tr.currency_exchange_rate AS amount_original
  , o.accepted_amount + COALESCE(txo.tax_amount, 0) AS accepted_amount
  , (o.accepted_amount + COALESCE(txo.tax_amount, 0))
    * tr.currency_exchange_rate AS accepted_amount_original
  , o.converted_amount + COALESCE(txo.tax_amount, 0) AS converted_amount
  , 100*(o.converted_amount+COALESCE(txo.tax_amount,0)) AS converted_amount_cent
  , CASE WHEN o.date_paid IS NOT NULL OR tr.app_name = 'b2b' THEN 'paid' ELSE 'unpaid' END AS is_paid_string
  , COALESCE(
    NULLIF(o.converted_amount, 0)
    , NULLIF(o.accepted_amount, 0)
    , o.original_amount
  )
    - COALESCE(d_o.amount, 0) -- donation amount
    + CASE WHEN prco.is_promo_code_valid = 1 THEN COALESCE(po.promo_amount, 0) ELSE 0 END -- promo amount
    + COALESCE(txo.tax_amount, 0) -- tax amount

  , ROUND(COALESCE(
        NULLIF(o.converted_amount, 0)
        , NULLIF(o.accepted_amount, 0)
        , 0
      ) -- base_price
      - COALESCE(d_o.amount, 0) -- donation amount
      + CASE WHEN prco.is_promo_code_valid = 1 THEN COALESCE(po.promo_amount, 0) ELSE 0 END -- promo amount
      + COALESCE(txo.tax_amount, 0) -- tax amount
    , 2) AS paid_amount
  , ROUND(COALESCE(
        NULLIF(o.converted_amount, 0)
        , NULLIF(o.accepted_amount, 0)
        , 0
      ) -- base_price
      - COALESCE(d_o.amount, 0) -- donation amount
      + CASE WHEN prco.is_promo_code_valid = 1 THEN COALESCE(po.promo_amount, 0) ELSE 0 END -- promo amount
      + COALESCE(txo.tax_amount, 0) -- tax amount
    , 2) * tr.currency_exchange_rate AS paid_amount_original
  , ROUND(COALESCE(
        NULLIF(o.converted_amount, 0)
        , NULLIF(o.accepted_amount, 0)
        , 0
      ) -- base_price
      - COALESCE(d_o.amount, 0) -- donation amount
      + CASE WHEN prco.is_promo_code_valid = 1 THEN COALESCE(po.promo_amount, 0) ELSE 0 END -- promo amount
      + COALESCE(txo.tax_amount, 0) -- tax amount
    , 2) AS paid_amount_2
  , ROUND(COALESCE(
        NULLIF(o.converted_amount, 0)
        , NULLIF(o.accepted_amount, 0)
        , 0
      ) -- base_price
      - COALESCE(d_o.amount, 0) -- donation amount
      + CASE WHEN prco.is_promo_code_valid = 1 THEN COALESCE(po.promo_amount, 0) ELSE 0 END -- promo amount
      + COALESCE(txo.tax_amount, 0) -- tax amount
    , 2) * tr.currency_exchange_rate AS paid_amount_2_original
  /*, ROUND(COALESCE(
        NULLIF(o.converted_amount, 0)
        , NULLIF(o.accepted_amount, 0)
        , 0
      ) -- base_price
      - COALESCE(d_o.amount, 0) -- donation amount
      + COALESCE(po.promo_amount, 0) -- promo amount
      + COALESCE(txo.tax_amount, 0) -- tax amount
    , 2) AS pl_paid_amount_2 -- identical to paid_amount_2??*/
  , COALESCE(o.converted_amount, o.accepted_amount)
    + CASE WHEN prco.is_promo_code_valid = 1 THEN COALESCE(po.promo_amount, 0) ELSE 0 END AS net_amount
  , (COALESCE(o.converted_amount, o.accepted_amount)
    + CASE WHEN prco.is_promo_code_valid = 1 THEN COALESCE(po.promo_amount, 0) ELSE 0 END) * tr.currency_exchange_rate AS net_amount_original
  , o.original_amount
  , o.original_amount * tr.currency_exchange_rate AS original_amount_original
  , tr.original_currency
  , tr.currency_exchange_rate

  , ret.retoure_description
  , ret.retoure_reason_detail
  , ret.retoure_reason_id
  , COALESCE(ret.retoure_warranty = 1, FALSE) AS retoure_is_warranty
  , rr.name AS retoure_reason_type

  , orh_c.rsp AS rsp_trade_created
  , orh_p.rsp AS rsp_trade_set_paid

  , tc.name AS all_tracking_campaign
  , tcg.name AS tracking_campaign_group

  , oc.condition_original
  , sq.answer_text AS simlock_net_customer_answer
  , bcq.answer_text AS battery_condition_customer_answer
  , lsc.last_changed_at AS latest_offer_state_change_at
  , lscrts.last_changed_at AS latest_offer_state_change_at_rts
  , opcv_other.value AS product_check_field_other
  , opcv_ts.value AS product_check_field_technical_state
  , os.name AS offer_state
  , iosb.value AS ios_battery_capacity
  , fsocs.first_stock_id
  , flsi.first_stock_in
  , flsi.first_stock_out
  , flsi.last_stock_in
  , flsi.last_stock_out
  , flsi.first_clarification_start
  , flsi.first_clarification_end
  , fsocs.first_clarification_accepted
  , fsocs.first_stock_out_channel
  , hdd.value AS hdd_size
  , hw_branding.value AS hw_branding
  , sw_branding.value AS sw_branding
  , simlock.value AS simlock
  , CASE
      WHEN tr.is_b2b_tax AND (NOT txo.offer_id IS NULL)
        THEN txo.tax / 100
      ELSE tr.landingpage_tax
    END AS tax
  , tr.app_name as application

  -- business_channel_offer and related fields testing deploy
  , oc.business_channel AS business_channel
  , COALESCE(du.usage, 'RE') AS usage
  , bct.name AS business_channel_type

  -- device
--  , CONCAT_WS(' '
--      , manu.name
--      , dev.model
--      , dev.edition
--      , dev.capacity
--    ) AS device
  , COALESCE(manu.name, '')||' '||COALESCE(dev.model,'')||' '||COALESCE(dev.edition,'')||' '||COALESCE(dev.capacity, '') AS device
  , manu.name AS manufactor_name
  , manu.id AS manufactor_id
  , CASE
      WHEN dev.edition IS NULL THEN dev_s.name
      ELSE  dev_s.name||' / '||dev.edition --concat(dev_s.name, ' / ', dev.edition)
    END AS series
  , cat.name AS category
  , cat.id AS category_id
  , dev.model
  , dev.id AS device_id
  , dev.capacity
  , dev.edition

  -- promo information
  , po.promo_amount
  , pc.code AS promo_code -- 35
  , pc.name AS promo_name -- 628
  , pc.value AS promo_value -- 36
  , CASE
      WHEN pc.value_type = 2 THEN 'Prozent'
      ELSE 'EUR'
    END AS promo_value_type -- 37

  -- donation information
  , d_o.amount AS donation_amount
  , d_o.amount * tr.currency_exchange_rate AS donation_amount_original
  , do_p.name AS donation_project

  -- stock aggregate information
  --, sm.stock_count AS number_of_stocks must be equal to sm.stock_in_count
  , sm.stock_in_count AS number_of_stocks
  , sm.stock_out_count AS number_of_stock_outs
  , sm.latest_stock_in
  , sm.latest_stock_out
  , sm.is_stock_out
  , sm.latest_stock_id AS id_of_active_stock_for_offer

FROM {{ ref('obj_wkfs.offer_cleaned') }} AS o
  -- aggregated stock information per offer
  LEFT JOIN {{ ref('wkfs_offer_stock_movements') }} AS sm
    ON sm.offer_id = o.id

  -- information based on the trade
  LEFT JOIN {{ ref('obj_wkfs_trade') }} AS tr
    ON tr.transaction_id = o.trade_id

  -- information based on the business_channel
  LEFT JOIN {{ ref('obj_wkfs.business_channel_offer_cleaned') }} AS bco
    ON bco.offer_id = o.id -- 1-1 relation
    LEFT JOIN {{ ref('obj_wkfs.business_channel_cleaned') }} AS bc
      ON bc.id = bco.business_channel_id
      LEFT JOIN {{ ref('obj_wkfs.business_channel_type_cleaned') }} AS bct
        ON bct.id = bc.business_channel_type_id
      LEFT JOIN {{ ref('obj_wkfs.calculation_cleaned') }} AS calc
        ON calc.id = bc.calculation_id
        LEFT JOIN {{ ref('obj_wkfs.device_usage_cleaned') }} AS du
          ON du.id = calc.usage_id

  -- information based on the device
  LEFT JOIN {{ ref('obj_wkfs.device_cleaned') }} AS dev
    ON dev.id = COALESCE(NULLIF(o.product_check_device_id, 0), o.device_id)
    LEFT JOIN {{ ref('obj_wkfs.manufactor_cleaned') }} AS manu
      ON manu.id = dev.manufactor_id
      LEFT JOIN {{ ref('obj_wkfs.category_cleaned') }} AS cat
        ON cat.id = manu.category_id
    LEFT JOIN {{ ref('obj_wkfs.serie_cleaned') }} AS dev_s
      ON dev_s.id = dev.serie_id

  -- CTEs
  LEFT JOIN refurbishments AS refs
    ON refs.offer_id = o.id
  LEFT JOIN retoures AS ret
    ON ret.offer_id = o.id
  LEFT JOIN original_condition AS oc
    ON oc.offer_id = o.id
  LEFT JOIN first_last_stock_in AS flsi
    ON flsi.offer_id = o.id
  LEFT JOIN first_stock_out_channel_status AS fsocs
    ON fsocs.offer_id = o.id

  /* questions and answers */
  LEFT JOIN {{ ref('obj_wkfs_question_answer') }} AS sq
    ON sq.calculation_code = 'SIMLOCK'
    AND sq.offer_id = o.id
  LEFT JOIN {{ ref('obj_wkfs_question_answer') }} AS bcq
    ON bcq.calculation_code = 'BATTERY_CONDITION'
    AND bcq.offer_id = o.id

  LEFT JOIN {{ ref('wkfs_offer_last_state_change')}} AS lsc
    ON lsc.offer_id = o.id

  LEFT JOIN {{ ref('wkfs_offer_last_state_change_rts')}} AS lscrts
    ON lscrts.offer_id = o.id

  LEFT JOIN {{ ref('obj_wkfs.device_condition_cleaned') }} AS dc
    ON dc.id = COALESCE(o.product_check_device_condition, o.condition)

  LEFT JOIN {{ ref('obj_wkfs.tax_offer_cleaned') }} AS txo
    ON txo.offer_id = o.id

  -- donation information
  LEFT JOIN {{ ref('obj_wkfs.donation_offer_cleaned') }} AS d_o
    ON d_o.offer_id = o.id
    LEFT JOIN {{ ref('obj_wkfs.donation_project_cleaned') }} AS do_p
      ON do_p.id = d_o.donation_project_id

  -- promo information
  LEFT JOIN {{ ref('obj_wkfs.promo_offer_cleaned') }} AS po
    ON po.offer_id = o.id
    LEFT JOIN {{ ref('obj_wkfs.promo_code_cleaned') }} AS pc
      ON pc.id = po.promo_id

  LEFT JOIN {{ ref('obj_wkfs.retoure_reason_cleaned') }} AS rr
    ON rr.id = ret.retoure_reason_id

  LEFT JOIN {{ ref('obj_wkfs.offer_rsp_history_cleaned') }} AS orh_c
    ON orh_c.identifier = 1 -- identifier trade created
    AND orh_c.offer_id = o.id
  LEFT JOIN {{ ref('obj_wkfs.offer_rsp_history_cleaned') }} AS orh_p
    ON orh_p.identifier = 2 -- identifier offer set paid
    AND orh_p.offer_id = o.id
  
  LEFT JOIN {{ ref('obj_wkfs.offer_offer_state_cleaned') }} AS oos
    ON oos.offer_id = o.id
    LEFT JOIN {{ ref('obj_wkfs.offer_state_cleaned') }} AS os
      ON os.id = oos.offer_state_id

  LEFT JOIN {{ ref('wkfs_promo_code_valid') }} AS prco
    ON prco.id = o.id

  LEFT JOIN {{ ref('obj_wkfs.advertising_offer_cleaned') }} AS ao
    ON ao.offer_id = o.id
    LEFT JOIN {{ ref('obj_wkfs.advertising_channel_cleaned') }} AS ac
      ON ac.id = ao.advertising_channel_id
      LEFT JOIN {{ ref('obj_wkfs.tracking_campaign_cleaned') }} AS tc
        ON tc.id = ac.tracking_campaign_id
        LEFT JOIN {{ ref('obj_wkfs.tracking_campaign_group_cleaned') }} AS tcg
          ON tcg.id = tc.tracking_campaign_group_id

  -- various current product check values
  LEFT JOIN {{ ref('obj_wkfs_offer_product_check_value_current') }} AS opcv_other
    ON opcv_other.product_check_field_id = 17 -- other
    AND opcv_other.offer_id = o.id
  LEFT JOIN {{ ref('obj_wkfs_offer_product_check_value_current') }} AS opcv_ts
    ON opcv_ts.product_check_field_id = 7 -- technical state
    AND opcv_ts.offer_id = o.id
  LEFT JOIN {{ ref('obj_wkfs_offer_product_check_value_current') }} AS iosb
    ON iosb.product_check_field_id = 119 -- ios battery
    AND iosb.offer_id = o.id
  LEFT JOIN {{ ref('obj_wkfs_offer_product_check_value_current') }} AS hw_branding
    ON hw_branding.product_check_field_id = 9 -- hardare branding
    AND hw_branding.offer_id = o.id
  LEFT JOIN {{ ref('obj_wkfs_offer_product_check_value_current') }} AS sw_branding
    ON sw_branding.product_check_field_id = 8 -- software branding
    AND sw_branding.offer_id = o.id
  LEFT JOIN {{ ref('obj_wkfs_offer_product_check_value_current') }} AS simlock
    ON simlock.product_check_field_id = 14 -- simlock
    AND simlock.offer_id = o.id
  LEFT JOIN {{ ref('obj_wkfs_offer_product_check_value_current') }} AS hdd
    ON hdd.product_check_field_id = 31 -- hdd size
    AND hdd.offer_id = o.id


-- don't delete below commented code
--  {% if is_incremental() %}
--   where o.id >= (select COALESCE(max(id::integer),0) from {{ this }})
--  {% endif %}