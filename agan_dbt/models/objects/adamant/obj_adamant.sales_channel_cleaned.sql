select
	convert(numeric,case id when '' then null else id end) as id,
	case "name" when '' then null else "name" end as name,
	convert(numeric,case warranty_allowed when '' then null else warranty_allowed end) as warranty_allowed,
	case "language" when '' then null else "language" end as language
from 
	raw_adamant.sales_channel