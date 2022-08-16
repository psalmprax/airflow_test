select *
from
(

with cte1 as
(

select *
from

	(
	select
		ows.id,
		ows.stock_out_price + ows.stock_out_shipping_costs as rechnungssumme,
		ows.stock_in as stock_in_date ,
--		date_part('day', ows.stock_out :: timestamp - ows.stock_in :: timestamp) as differenz_stockout_stockin,
		datediff('day', ows.stock_in, ows.stock_out) as differenz_stockout_stockin,
		ows.offer_id as offer_id_report_stock,
		owo.offer_state as offer_state,
		owo.last_stock_in as eingang_retoure ,
		ows.stock_out_info  as lieferschein,
		owo.condition_title as zustand_vk,
		owo.business_channel_type as ankaufskanal,
		owo.paid_amount as ankaufswert,
		dense_rank() over (partition by offer_id order by ows.id desc) as rnk, -- this filters for the most recent return
		count(offer_id) over (partition by offer_id) as cnt  -- if the offer_id appears only once then the item has never been returned
	from
		{{ ref('obj_wkfs_stock') }} ows
	left join
		{{ ref('obj_wkfs_offer') }} owo
		on ows.offer_id = owo.id
	where ows.stock_out_channel not in ('Ersatzteile intern', 'back2seller') -- exclude these status
	and stock_out_info ~ '^\d{1,6}$' -- the stock_out_info for a return has to be 1-6 digits with no letters/special characters
	) as sub
where rnk = 1
and cnt > 1
),

cte2 as
(

select *
from

	(
	select
		oao.order_id,
		oai.created_at as re_datum,
		oao.reference as bestellnummer,
		oaoid.device_id  as device_id,
		oaoid.order_item_id,
		oad.reference as offer_id_dad,
		dense_rank() over (partition by oad.reference order by oao.order_id desc) as rnk3,
		oad.device_name,
		oao.invoice_id as re_nummer,
		oap.gateway_name as zahlart,
		oap.reference as payment_reference,
		oao.country_code as vertriebsland,
		oac.firstname || ' ' || oac.lastname as kundenname,
		oas.shipping_name as versandart,
		case
			when oao.confirmation_sent_at is not null
			then True
			else False
		end as has_warranty
		from
			{{ ref('obj_adamant_orders') }} oao
		left join {{ ref('obj_adamant_order_items') }} oaoi
			on oao.order_id = oaoi.order_id
		left join {{ ref('obj_adamant_payments') }} oap
			on oao.payment_id = oap.payment_id
		left join {{ ref('obj_adamant_order_item_details') }} oaoid
			on oaoi.order_item_id = oaoid.order_item_id
		left join {{ ref('obj_adamant_invoices') }} oai
			on oao.invoice_id = oai.invoice_id
		left join {{ ref('obj_adamant_devices') }} oad
			on oaoid.device_id = oad.device_id
		left join {{ ref('obj_adamant_customers') }} oac
			on oao.customer_id = oac.customer_id
		left join {{ ref('obj_adamant_shippings') }} oas
			on oao.shipping_id = oas.shipping_id
		where oai.created_at is not null -- if invoice date is empty then not a return
	) as subtable
where rnk3 = 1
),

cte3 as
(

select *
	from cte1
	left join cte2
	on cte1.offer_id_report_stock = cte2.offer_id_dad   -- reference from device table = TA number = offer_id
)

select
cte3.id,
cte3.stock_in_date ,
cte3.differenz_stockout_stockin,
cte3.kundenname,
--cte3.offer_id_report_stock,
cte3.offer_state,
cte3.eingang_retoure ,
cte3.lieferschein,
cte3.zustand_vk,
cte3.ankaufskanal,
cte3.ankaufswert,
cte3.order_id,
cte3.re_datum,
cte3.bestellnummer,
cte3.device_id,
cte3.device_name,
cte3.re_nummer,
cte3.rechnungssumme,
cte3.zahlart,
cte3.versandart,
cte3.payment_reference,
cte3.vertriebsland,
cte3.has_warranty
from cte3
order by stock_in_date desc , offer_id_report_stock desc) s