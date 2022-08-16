 select 
 business_channel, business_channel_type_original,
 business_channel_name, business_channel_type, 
 offer_id_original,  stock_out_channel as sales_channel
 from 
 (
	 select
           distinct business_channel
         , business_channel_type as business_channel_type_original
         , id as offer_id_original
	 from {{ ref('obj_wkfs_offer') }} owo

 ) result
 
 left join {{ ref('report_cost_of_sales_margin') }} rcosm
 on result.offer_id_original = rcosm.offer_id
 left join {{ ref('dim_business_channels') }} dbc
 on dbc.business_channel_name = result.business_channel
 and dbc.business_channel_type = result.business_channel_type_original 