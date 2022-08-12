select
	convert(numeric, case id when '' then null else id end) as id,
	case
		code when '' then null
		else code
	end as code,
	case
		"name" when '' then null
		else "name"
	end as "name"
from
	raw_wkfs.retoure_reason