select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	case
		part when '' then null
		else part
	end as part,
	convert(decimal(15,2), case costs when '' then 0.00 else costs end) as costs
from
	raw_wkfs.offer_refurbishment