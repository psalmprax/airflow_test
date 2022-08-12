SELECT
	offer_id
	, COUNT(id) AS stock_count
	, COUNT(stock_in) AS stock_in_count
	, COUNT(stock_out) AS stock_out_count
	, MAX(stock_in) AS latest_stock_in
	, MAX(stock_out) AS latest_stock_out
	, COUNT(stock_in) = COUNT(stock_out) AS is_stock_out
	, MAX(id) AS latest_stock_id
FROM {{ ref('obj_wkfs_stock') }}
GROUP BY 1