with results as (
select
	convert(numeric, case transaction_id when '' then null else transaction_id end) as transaction_id,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	convert(numeric, case sf_guard_user_id when '' then null else sf_guard_user_id end) as sf_guard_user_id,
	convert(numeric, case created_by_id when '' then null else created_by_id end) as created_by_id,
	convert(numeric, case user_id when '' then null else user_id end) as user_id,
	convert(numeric, case payment_id when '' then null else payment_id end) as payment_id,
	convert(numeric, case pick_up_adress_id when '' then null else pick_up_adress_id end) as pick_up_adress_id,
	case
		dhl_identcode when '' then null
		else dhl_identcode
	end as dhl_identcode,
	convert(numeric, case accepted when '' then null else accepted end) as accepted,
	convert(numeric, case application_id when '' then null else application_id end) as application_id,
	convert(numeric, case location_id when '' then null else location_id end) as location_id,
	convert(numeric, case deleted when '' then null else deleted end) as deleted,
	convert(numeric, case fastlane when '' then null else fastlane end) as fastlane,
	convert(numeric, case currency_rate_id when '' then null else currency_rate_id end) as currency_rate_id,
	convert(numeric, case private_use when '' then null else private_use end) as private_use,
	case
		"comment" when '' then null
		else "comment"
	end as "comment",
	case
		"server" when '' then null
		else "server"
	end as "server",
	case
		repair_api_backlink when '' then null
		else repair_api_backlink
	end as repair_api_backlink
from
	{{ source("wkfs","trade") }}
)

select * from results