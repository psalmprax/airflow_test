with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	case
		"usage" when '' then null
		else "usage"
	end as "usage",
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		"type" when '' then null
		else "type"
	end as "type",
	convert(numeric, case required when '' then null else required end) as required,
	case
		validation_class when '' then null
		else validation_class
	end as validation_class
from
	raw_wkfs.add_userfield
)

select * from results