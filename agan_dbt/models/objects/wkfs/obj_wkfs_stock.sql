SELECT
    stock.id
    -- '0000-00-00 00:00:00' cannot be parsed to timestamp
--  , NULLIF(stock.stock_out, '0000-00-00 00:00:00')::TIMESTAMP WITH TIME ZONE
   , NULLIF(stock.stock_out::date, '0000-01-01 00:00:00'::date) AS stock_out
  {{ select_all_except(
    'wkfs',
    'stock',
    [
        'id'
      , 'stock_out'
      ,"dag"
      ,"data_interval_start"
      ,"data_interval_end"
      ,"execution_date"
      ,"next_execution_date"
      ,"prev_data_interval_start_success"
      ,"prev_data_interval_end_success"
      ,"prev_execution_date"
      ,"prev_start_date_success"
      ,"prev_execution_date_success"
    ],
    False,
    'stock')
  }}

  , lc.is_clarification_questionnaire
  , lc.is_clarification_technical
  , lc.is_clarification_optical
  , lc.is_clarification_general_data
  , COALESCE(lc.status, 'not checked') AS status
  , lc.boxnumber
  , cl.calculated AS calculated_amount
  , cl.accepted_amount
  , cl.datesubmit AS clarification_start_at
  , cl.dateclosed AS clarification_end_at

  , r.costs AS repair_costs
  , r.out AS repair_out_at
  , r.returned AS repair_stop_at
  , r.remarks AS repair_description
  , r.start AS repair_start_at
  , r.new_optical_state AS repair_new_optical_state
  , COALESCE(r.battery_changed = 1, FALSE) AS repair_is_battery_changed
  , COALESCE(r.repaired = 1, FALSE) AS is_repaired
  , rc.name AS repair_contractor_name

  , pc.color
  , pc.serial
  , pc.ovp

  , soi.info AS stock_out_info
  , soi.price AS stock_out_price
  , soi.shippingcosts AS stock_out_shipping_costs
  , soi.price * so_bc_fx.fx_rate AS stock_out_price_original
  , soi.shippingcosts * so_bc_fx.fx_rate AS stock_out_shipping_costs_original
  , so_bc.outlet_name AS stock_out_channel

--  , NULLIF(stock.stock_out, '0000-00-00 00:00:00')::TIMESTAMPTZ - stock.stock_in::TIMESTAMPTZ AS storage_time
   , NULLIF(stock.stock_out::date, '0000-01-01 00:00:00'::date)
    - NULLIF(stock.stock_in::date, '0000-01-01 00:00:00'::date) AS storage_time
FROM {{ ref('obj_wkfs.stock_cleaned') }} AS stock

  LEFT JOIN {{ ref('obj_wkfs_product_check') }} AS lc
    ON lc.order_of_clarify_checks_per_stock_desc = 1
    AND lc.stock_id = stock.id
    LEFT JOIN {{ ref('wkfs_last_clarification_per_product_check_id_in') }} AS cl
      ON cl.product_checks_id_in = lc.id

  LEFT JOIN {{ ref('wkfs_latest_repair_per_stock') }} AS lrps
    ON lrps.stock_id = stock.id
    LEFT JOIN {{ ref('obj_wkfs.repair_cleaned') }} AS r
      ON r.check_result_actions_id = lrps.wkfs_latest_repair_per_stock
      LEFT JOIN {{ ref('obj_wkfs.contractor_cleaned') }} AS rc
        ON rc.id = r.contractor_id

  LEFT JOIN {{ ref('wkfs_last_product_check_per_stock') }} AS lpc
    ON lpc.stock_id = stock.id
    LEFT JOIN {{ ref('obj_wkfs_product_check') }} AS pc
      ON pc.id = lpc.product_check_id

  LEFT JOIN {{ ref('obj_wkfs.stock_out_info_cleaned') }} AS soi
    ON soi.stock_id = stock.id
    LEFT JOIN {{ ref('obj_wkfs.business_channel_cleaned') }} AS so_bc
      ON so_bc.id = soi.business_channel_id
      LEFT JOIN {{ ref('wkfs_current_currency_rates') }} AS so_bc_fx
        ON so_bc_fx.currency_id = so_bc.currency_id