select
	convert(numeric, case id when '' then null else id end) as id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		country when '' then null
		else country
	end as country,
	convert(numeric, case disabled when '' then null else disabled end) as disabled
from
	raw_wkfs.province