with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		code when '' then null
		else code
	end as code,
	convert(numeric, case do_update_customer when '' then null else do_update_customer end) as do_update_customer,
	convert(numeric, case is_setting_paid when '' then null else is_setting_paid end) as is_setting_paid,
	case
		manipulations when '' then null
		else manipulations
	end as manipulations,
	convert(numeric, case is_syncable when '' then null else is_syncable end) as is_syncable
from
	{{ source("wkfs","offer_state") }}
)

select * from results