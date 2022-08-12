select
	convert(numeric, case user_id when '' then null else user_id end) as user_id,
	case
		firstname when '' then null
		else firstname
	end as firstname,
	case
		lastname when '' then null
		else lastname
	end as lastname,
	case
		email when '' then null
		else email
	end as email,
	case
		street when '' then null
		else street
	end as street,
	case
		"number" when '' then null
		else "number"
	end as "number",
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
		phone when '' then null
		else phone
	end as phone,
	case
		company when '' then null
		else company
	end as company,
	case
		sapid when '' then null
		else sapid
	end as sapid
from
	raw_wkfs.sf_guard_user_profile