select
	convert(numeric, case payment_id when '' then null else payment_id end) as payment_id,
	case
		simphone when '' then null
		else simphone
	end as simphone,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
    {{ source("wkfs","payment_simyo_credits")}}

--	raw_wkfs.payment_simyo_credits