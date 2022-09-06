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
		dni when '' then null
		else dni
	end as dni,
	case
		bic when '' then null
		else bic
	end as bic,
	case
		iban when '' then null
		else iban
	end as iban,
	case
		initialization_vector when '' then null
		else initialization_vector
	end as initialization_vector,
	case
		international_bank_account_number when '' then null
		else international_bank_account_number
	end as international_bank_account_number,
	case
		account when '' then null
		else account
	end as account,
	case
		foreigner_identity_number when '' then null
		else foreigner_identity_number
	end as foreigner_identity_number,
	case
		nie when '' then null
		else nie
	end as nie,
	case
		identity_number when '' then null
		else identity_number
	end as identity_number,
	null as iban_decrypt,
	null as account_decrypt,
    null as fin_decrypt
from
    {{ source("wkfs","payment_direct_debit_spain")}}
--	raw_wkfs.payment_direct_debit_spain