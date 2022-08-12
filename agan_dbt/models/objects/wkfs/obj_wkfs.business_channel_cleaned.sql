with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	convert(numeric, case sf_guard_user_id when '' then null else sf_guard_user_id end) as sf_guard_user_id,
	convert(numeric, case business_channel_type_id when '' then null else business_channel_type_id end) as business_channel_type_id,
	case
		outlet_name when '' then null
		else outlet_name
	end as outlet_name,
	convert(numeric, case calculation_id when '' then null else calculation_id end) as calculation_id,
	convert(numeric, case deleted when '' then null else deleted end) as deleted,
	convert(numeric, case currency_id when '' then null else currency_id end) as currency_id,
	convert(numeric, case use_in_device_list when '' then null else use_in_device_list end) as use_in_device_list
from
	{{ source("wkfs","business_channel") }}
)

select * from results