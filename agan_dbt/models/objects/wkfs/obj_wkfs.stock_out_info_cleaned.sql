select
	convert(numeric, case stock_id when '' then null else stock_id end) as stock_id,
	convert(numeric, case business_channel_id when '' then null else business_channel_id end) as business_channel_id,
	case
		info when '' then null
		else info
	end as info,
	COALESCE(convert(decimal(15,2), case price when '' then null else price end),0.00) as price,
	COALESCE(convert(decimal(15,2), case shippingcosts when '' then null else shippingcosts end),0.00) as shippingcosts,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
	raw_wkfs.stock_out_info