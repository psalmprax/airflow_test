select
	convert(numeric, case id when '' then null else id end) as id,
	case
		description when '' then null
		else description
	end as description,
	case
		image when '' then null
		else image
	end as image,
	case
		"name" when '' then null
		else "name"
	end as "name",
	convert(numeric, case deleted when '' then null else deleted end) as deleted,
	convert(numeric, case donation_project_category_id when '' then null else donation_project_category_id end) as donation_project_category_id,
	case
		link when '' then null
		else link
	end as link,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
	raw_wkfs.donation_project