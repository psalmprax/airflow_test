select
	convert(numeric, case id when '' then null else id end) as id,
	case
		email when '' then null
		else email
	end as email,
	case
		firstname when '' then null
		else firstname
	end as firstname,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		description when '' then null
		else description
	end as description,
	convert(timestamp, case mail_submitted when '' then null else mail_submitted end) as mail_submitted,
	case
		dhl_identcode when '' then null
		else dhl_identcode
	end as dhl_identcode,
	convert(numeric, case stock_out_id when '' then null else stock_out_id end) as stock_out_id,
	convert(numeric, case stock_in_id when '' then null else stock_in_id end) as stock_in_id,
	convert(numeric, case warranty when '' then null else warranty end) as warranty,
	convert(numeric, case retoure_reason_id when '' then null else retoure_reason_id end) as retoure_reason_id,
	case
		retoure_case when '' then null
		else retoure_case
	end as retoure_case,
	convert(numeric, case catalogue_country_id when '' then null else catalogue_country_id end) as catalogue_country_id,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	convert(numeric, case sf_guard_user_id when '' then null else sf_guard_user_id end) as sf_guard_user_id,
	case
		decision when '' then null
		else decision
	end as decision,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
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
		retoure_reason_text when '' then null
		else retoure_reason_text
	end as retoure_reason_text
from
	raw_wkfs.retoure