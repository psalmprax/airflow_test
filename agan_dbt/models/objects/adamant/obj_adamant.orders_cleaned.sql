with results as (
select
	convert(numeric,case id when '' then null else id end) as id,
	convert(numeric,case shipping_address_id when '' then null else shipping_address_id end) as shipping_address_id,
	convert(numeric,case business_channel_id when '' then null else business_channel_id end) as business_channel_id,
	convert(numeric,case customer_id when '' then null else customer_id end) as customer_id,
	case reference when '' then null else reference end as reference,
	convert(timestamp, case sold_at when '' then null else sold_at end) as sold_at, 
	case status when '' then null else status end as status,
	convert(decimal(15,2),case shipping_price when '' then null else shipping_price end) as shipping_price,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at, 
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at, 
	convert(numeric,case sales_channel_id when '' then null else sales_channel_id end) as sales_channel_id,
	convert(timestamp, case invitation_sent_at when '' then null else invitation_sent_at end) as invitation_sent_at,
	case invoice_id when '' then null else invoice_id end as invoice_id,
	convert(numeric,case shipping_id when '' then null else shipping_id end) as shipping_id,
	convert(numeric,case exported when '' then null else exported end) as exported,
	convert(numeric,case payment_id when '' then null else payment_id end) as payment_id,
	convert(timestamp, case confirmation_sent_at when '' then null else confirmation_sent_at end) as confirmation_sent_at,
	case "comment" when '' then null else "comment" end as comment
from 
	{{ source("adamant","orders") }}
)

select * from results