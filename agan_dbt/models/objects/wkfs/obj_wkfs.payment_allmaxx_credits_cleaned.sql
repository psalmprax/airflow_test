select
	convert(numeric, case payment_id when '' then null else payment_id end) as payment_id,
	case
		allmaxxid when '' then null
		else allmaxxid
	end as allmaxxid,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
	raw_wkfs.payment_allmaxx_credits