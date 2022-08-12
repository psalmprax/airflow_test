with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case application_id when '' then null else application_id end) as application_id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	convert(numeric, case tracking_campaign_id when '' then null else tracking_campaign_id end) as tracking_campaign_id,
	convert(numeric, case advertising_medium_id when '' then null else advertising_medium_id end) as advertising_medium_id,
	case
		"text" when '' then null
		else "text"
	end as "text",
	case
		refer when '' then null
		else refer
	end as refer,
	convert(numeric, case clicks when '' then null else clicks end) as clicks,
	convert(numeric, case deleted when '' then null else deleted end) as deleted
from
	{{ source("wkfs","advertising_channel") }}
)

select * from results