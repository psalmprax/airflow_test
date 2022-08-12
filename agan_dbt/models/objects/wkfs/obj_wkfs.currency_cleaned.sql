select
	convert(numeric, case id when '' then null else id end) as id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		abbreviation when '' then null
		else abbreviation
	end as abbreviation,
	case
		symbol when '' then null
		else symbol
	end as symbol
from
	raw_wkfs.currency