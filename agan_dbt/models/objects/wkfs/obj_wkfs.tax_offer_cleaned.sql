with results as (
select
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	convert(numeric, case tax when '' then null else tax end) as tax,
	convert(double precision, case tax_amount when '' then null else tax_amount end) as tax_amount,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
	{{ source("wkfs","tax_offer") }}
)
select * from results