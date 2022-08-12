with results as (
select
	convert(numeric,case id when '' then null else id end) as id,
	case "name" when '' then null else "name" end as name,
	convert(numeric,case back_office_id when '' then null else back_office_id  end) as back_office_id 
from 
	{{ source("adamant","business_channel") }}
)

select * from results
