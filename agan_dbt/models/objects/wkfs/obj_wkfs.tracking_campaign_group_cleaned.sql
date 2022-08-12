select
	convert(numeric, case id when '' then null else id end) as id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	convert(numeric, case deleted when '' then null else deleted end) as deleted
from
	raw_wkfs.tracking_campaign_group