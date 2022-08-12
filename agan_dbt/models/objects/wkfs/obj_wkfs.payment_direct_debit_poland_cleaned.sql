select
	convert(numeric, case payment_id when '' then null else payment_id end) as payment_id,
	case
		account_number when '' then null
		else account_number
	end as account_number,
	case
		bank_name when '' then null
		else bank_name
	end as bank_name,
	case
		initialization_vector when '' then null
		else initialization_vector
	end as initialization_vector,
	case
		bank_identifier_code when '' then null
		else bank_identifier_code
	end as bank_identifier_code,
	case
		international_bank_account_number when '' then null
		else international_bank_account_number
	end as international_bank_account_number
from
	raw_wkfs.payment_direct_debit_poland