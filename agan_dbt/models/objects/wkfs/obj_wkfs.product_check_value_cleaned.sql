with results as (
select
	convert(numeric, case product_check_id when '' then null else product_check_id end) as product_check_id,
	convert(numeric, case product_check_field_id when '' then null else product_check_field_id end) as product_check_field_id,
	convert(numeric, case clarification when '' then null else clarification end) as clarification,
	convert(numeric, case defect when '' then null else defect end) as defect,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	case
		value when '' then null
		else value
	end as value
from
	{{ source("wkfs","product_check_value") }}
)

select * from results