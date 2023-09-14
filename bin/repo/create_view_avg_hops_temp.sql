create or replace view data_mart.avg_hops_ferm_temp as
select di.ingredient_name as hops
	,avg(lmb.temp) as avg_temp
from core.link_ingredients_beer lib
join core.link_method_beer lmb on lmb.dim_beer_id = lib.dim_beer_id
join core.dim_ingredients di on di.ingredient_id = lib.dim_ingredient_id
where lmb.stage = 'fermentation'
and di.ingredient_type = 'hops'
group by 1;