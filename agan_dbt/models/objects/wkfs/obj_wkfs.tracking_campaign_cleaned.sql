select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case tracking_campaign_group_id when '' then null else tracking_campaign_group_id end) as tracking_campaign_group_id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	convert(numeric, case deleted when '' then null else deleted end) as deleted,
	convert(timestamp, case date_start when '' then null else date_start end) as date_start,
	convert(timestamp, case date_end when '' then null else date_end end) as date_end,
	convert(numeric, case webtrekk when '' then null else webtrekk end) as webtrekk
from
	raw_wkfs.tracking_campaign