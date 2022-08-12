select
	convert(numeric, case id when '' then null else id end) as id,
	case
		email when '' then null
		else email
	end as email,
	convert(numeric, case store_id when '' then null else store_id end) as store_id
from
	raw_wkfs.trade_store_email