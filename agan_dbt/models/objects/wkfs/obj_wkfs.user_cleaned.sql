with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	case
		firstname when '' then null
		else firstname
	end as firstname,
	case
		lastname when '' then null
		else lastname
	end as lastname,
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
		email when '' then null
		else email
	end as email,
	case
		phone when '' then null
		else phone
	end as phone,
	case
		"password" when '' then null
		else "password"
	end as "password",
	convert(numeric, case optin when '' then null else optin end) as optin,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	convert(numeric, case is_uploaded_to_responsys when '' then null else is_uploaded_to_responsys end) as is_uploaded_to_responsys,
	case
		"number" when '' then null
		else "number"
	end as "number",
	case
		flat_number when '' then null
		else flat_number
	end as flat_number,
	convert(numeric, case gender when '' then null else gender end) as gender,
	convert(numeric, case province_id when '' then null else province_id end) as province_id,
	convert(timestamp, case birthdate when '' then null else birthdate end) as birthdate
from
	{{ source("wkfs","users") }}
)

select * from results