select
	convert(numeric, case id when '' then null else id end) as id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		"label" when '' then null
		else "label"
	end as "label",
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
	raw_wkfs.payment_type