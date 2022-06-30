insert into analytics_objects.obj_product_price
select pps.price, pps.anbieter, pps.device, pps.condition, pps.created_at, pps.am_pm
from analytics_objects.obj_product_price_stage pps
left join analytics_objects.obj_product_price pp on pps.price = pp.price
and pps.anbieter = pp.anbieter
and pps.device = pp.device
and pps.condition = pp.condition
and cast(pps.created_at as DATE) = cast(pp.created_at as DATE)
and pps.am_pm = pp.am_pm
where pp.price is null;

drop table if exists analytics_objects.obj_product_price_stage;