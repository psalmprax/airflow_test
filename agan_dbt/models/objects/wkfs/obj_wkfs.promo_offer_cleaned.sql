with results as (
select
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	convert(numeric, case promo_id when '' then null else promo_id end) as promo_id,
	convert(decimal(15,2), case promo_amount when '' then 0.00 else promo_amount end) as promo_amount,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
	{{ source("wkfs","promo_offer") }}
)

select * from results