select
	convert(numeric, case id when '' then null else id end) as id,
	case
		value when '' then null
		else value
	end as value,
	convert(timestamp, case valid_until when '' then null else valid_until end) as valid_until
from
	raw_wkfs.trade_whitelist