select
	convert(numeric, case id when '' then null else id end) as id,
	case
		value when '' then null
		else value
	end as value
from
	raw_wkfs.trade_blacklist