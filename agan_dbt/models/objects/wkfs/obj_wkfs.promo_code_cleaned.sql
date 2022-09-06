select
	convert(numeric, case id when '' then null else id end) as id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		code when '' then null
		else code
	end as code,
	convert(numeric, case is_unique when '' then null else is_unique end) as is_unique,
	convert(numeric, case min_amount_is_active when '' then null else min_amount_is_active end) as min_amount_is_active,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(numeric, case sf_guard_user_id when '' then null else sf_guard_user_id end) as sf_guard_user_id,
	convert(numeric, case value when '' then null else value end) as value,
	convert(numeric, case value_type when '' then null else value_type end) as value_type,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	convert(timestamp, case valid_until when '' then null else valid_until end) as valid_until,
	convert(numeric, case min_amount_value when '' then null else min_amount_value end) as min_amount_value
from
    {{ source("wkfs","promo_code")}}

--	raw_wkfs.promo_code