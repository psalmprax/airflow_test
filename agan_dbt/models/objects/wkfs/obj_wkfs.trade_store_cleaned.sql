select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case gender when '' then null else gender end) as gender,
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
		street_number when '' then null
		else street_number
	end as street_number,
	case
		zip_code when '' then null
		else zip_code
	end as zip_code,
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
		iban when '' then null
		else iban
	end as iban,
	case
		bic when '' then null
		else bic
	end as bic,
	case
		account_holder when '' then null
		else account_holder
	end as account_holder
from
	raw_wkfs.trade_store