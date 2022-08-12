select
	convert(numeric, case payment_id when '' then null else payment_id end) as payment_id,
	case
		swift_account_owner when '' then null
		else swift_account_owner
	end as swift_account_owner,
	case
		iban when '' then null
		else iban
	end as iban,
	case
		bic when '' then null
		else bic
	end as bic,
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
	raw_wkfs.payment_direct_debit_swift