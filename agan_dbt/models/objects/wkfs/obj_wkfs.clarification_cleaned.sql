with results as (
 select
	convert(numeric, case check_result_actions_id when '' then null else check_result_actions_id end) as check_result_actions_id,
	convert(numeric, case business_channel_id when '' then null else business_channel_id end) as business_channel_id,
	convert(numeric, case submitted when '' then null else submitted end) as submitted,
	convert(timestamp, case date_delay_to when '' then null else date_delay_to end) as date_delay_to,
	convert(timestamp, case datesubmit when '' then null else datesubmit end) as datesubmit,
	convert(timestamp, case dateclosed when '' then null else dateclosed end) as dateclosed,
	convert(decimal(15,2), case accepted_amount when '' then 0.00 else accepted_amount end) as accepted_amount,
	convert(decimal(15,2), case donation_amount when '' then 0.00 else donation_amount end) as donation_amount,
	convert(decimal(15,2), case calculated when '' then 0.00 else calculated end) as calculated,
	convert(numeric, case accepted when '' then null else accepted end) as accepted,
	convert(numeric, case closed when '' then null else closed end) as closed,
	convert(numeric, case closed_confirm when '' then null else closed_confirm end) as closed_confirm,
	convert(numeric, case converted_amount when '' then null else converted_amount end) as converted_amount,
	convert(numeric, case converted_offer_amount when '' then null else converted_offer_amount end) as converted_offer_amount,
	convert(numeric, case skipped when '' then null else skipped end) as skipped,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	case
		checksum when '' then null
		else checksum
	end as checksum,
	case
		answertext when '' then null
		else answertext
	end as answertext
from
	{{ source("wkfs","clarification") }}

)

select * from results