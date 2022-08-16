select
*
from
(
	select
        ows.id as stock_id , --obj_wkfs_stock
        owo.offer as offer_id, --obj_wkfs_offer
        owo.article_number , --obj_wkfs_offer
        owo.condition_id , --obj_wkfs_offer
        ows.status as checked_passed , --obj_wkfs_stock
        owo.offer_state as status_code_1 , --obj_wkfs_offer
        owpc.checked as check_time , -- obj_wkfs_product_check
        max(owpc.checked) over(partition by offer) as timestamp, -- obj_wkfs_product_check
        owo.is_paid_string as is_paid,  --obj_wkfs_offer
        owpc.warranty , -- obj_wkfs_product_check
        owpc.ebay_product_id, -- obj_wkfs_product_check
        owpc.boxnumber as box_id, -- obj_wkfs_product_check
        owpc.quality_control -- obj_wkfs_product_check

	from {{ ref('obj_wkfs_offer')}} owo
	left join {{ ref('obj_wkfs_stock')}} ows
	on owo.offer = cast(ows.offer_id as text)
	left join {{ ref('obj_wkfs_product_check')}} owpc
	on ows.id = owpc.stock_id
) df