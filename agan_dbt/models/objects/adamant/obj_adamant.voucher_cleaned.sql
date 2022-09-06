select
	convert(numeric,case id when '' then null else id end) as id,
	case code when '' then null else code end as code,
	COALESCE(convert(decimal(15,2),case amount when '' then null else amount end),0.00) as amount
from
	raw_adamant.voucher