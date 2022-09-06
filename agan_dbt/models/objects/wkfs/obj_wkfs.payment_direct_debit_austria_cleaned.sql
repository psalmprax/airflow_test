select
	convert(numeric, case payment_id when '' then null else payment_id end) as payment_id,
	case
		account_owner when '' then null
		else account_owner
	end as account_owner,
	case
		account_number when '' then null
		else account_number
	end as account_number,
	case
		bank_code when '' then null
		else bank_code
	end as bank_code,
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
	end as international_bank_account_number,
	null as iban_decrypt,
	null as bic_decrypt
from
    {{ source("wkfs","payment_direct_debit_austria")}}

--	raw_wkfs.payment_direct_debit_austria