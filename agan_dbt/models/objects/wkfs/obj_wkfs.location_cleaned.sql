select
	convert(numeric, case id when '' then null else id end) as id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		street when '' then null
		else street
	end as street,
	case
		zip when '' then null
		else zip
	end as zip,
	case
		city when '' then null
		else city
	end as city,
	case
		country when '' then null
		else country
	end as country,
	case
		leitcode_headline when '' then null
		else leitcode_headline
	end as leitcode_headline,
	case
		leitcode when '' then null
		else leitcode
	end as leitcode
from
	raw_wkfs.location