select
	convert(numeric,case id when '' then null else id end) as id,
	case "name" when '' then null else "name" end as name
from 
	raw_adamant.category
