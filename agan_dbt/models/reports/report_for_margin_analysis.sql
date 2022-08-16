SELECT
    id
	, offer_id
	, category
	, device
	, stock_in AS stock_in_at
	, stock_out AS stock_out_at
    , EXTRACT('week' FROM stock_out) AS stock_out_week
    , EXTRACT('year' FROM stock_out) AS stock_out_isoyear
--	, EXTRACT('isoyear' FROM stock_out) AS stock_out_isoyear
	, manufactor_name
	, stock_out_price
	, stock_out_channel
	, condition_id
	, condition_title
	, app_name
	, firstname
	, lastname
	, stock_out_shipping_costs
	, paid_amount
	, tax
	, promo_value
	, margin
	, storage_time
FROM {{ ref('report_stock') }}
WHERE NOT stock_out IS NULL
