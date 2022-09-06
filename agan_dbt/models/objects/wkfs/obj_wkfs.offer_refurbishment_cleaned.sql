select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	case
		part when '' then null
		else part
	end as part,
	COALESCE(convert(decimal(15,2), case costs when '' then null else costs end),0.00) as costs
from
	raw_wkfs.offer_refurbishment