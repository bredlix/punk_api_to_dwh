--create or replace view data_mart.avg_hops_ferm_temp as
select di.ingredient_name as hops
	,avg(lmb.temp) as avg_temp
from core.link_ingredients_beer lib 
join core.link_method_beer lmb on lmb.dim_beer_id = lib.dim_beer_id 
join core.dim_ingredients di on di.ingredient_id = lib.dim_ingredient_id
where lmb.stage = 'fermentation'
and di.ingredient_type = 'hops'
group by 1;



--create or replace view data_mart.avg_top_hops_ferm_temp as
with one as (
	select lib.dim_beer_id 
		,di.ingredient_name as hops
		,lmb.temp
		,sum(case when lib.unit in ('kilograms','kilogram','total') then lib.amount * 1000
			else lib.amount end) as sum_amount
	from core.link_ingredients_beer lib 
	join core.link_method_beer lmb on lmb.dim_beer_id = lib.dim_beer_id 
	join core.dim_ingredients di on di.ingredient_id = lib.dim_ingredient_id
	where lmb.stage = 'fermentation'
		and di.ingredient_type = 'hops'
	group by 1,2,3
	order by 1,4 desc),
two as (
	select *
		,row_number() over (partition by dim_beer_id order by sum_amount desc) as rn
	from one)
select hops
	,avg(temp) as avg_temp
from two
where rn = 1
group by 1