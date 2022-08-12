select
	convert(numeric,case id when '' then null else id end) as id,
	case "name" when '' then null else "name" end as "name",
	case roles when '' then null else roles end as roles
from 
	raw_adamant.fos_user_group