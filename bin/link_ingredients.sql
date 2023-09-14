create table core.link_ingredients_beer (
	id serial primary key,
	dim_beer_id int,
	dim_ingredient_id int,
	amount decimal,
	unit varchar,
	stage varchar,
	attr varchar,
	foreign key(dim_beer_id) references core.dim_beer (id),
	foreign key(dim_ingredient_id) references core.dim_ingredients (ingredient_id)
)
;


insert into core.link_ingredients_beer (
	dim_beer_id,
	dim_ingredient_id,
	amount,
	unit,
	stage,
	attr
)
select 
	db.id as dim_beer_id
	,di.ingredient_id as dim_ingredient_id
	,cast((hops::json->>'amount')::json->>'value' as decimal) as amount
	,(hops::json->>'amount')::json->>'unit' as unit
	,hops::json->>'add' as stage
	,hops::json->>'attribute' as attr
from raw_data.beers b
join json_array_elements(b.ingredients::json->'hops') as hops on TRUE
join core.dim_beer db on db.beer_id = b.id
join core.dim_ingredients di  on di.ingredient_name = hops::json->>'name'
union
select 
	db.id as dim_beer_id
	,di.ingredient_id as dim_ingredient_id
	,cast((malt::json->>'amount')::json->>'value' as decimal) as amount
	,(malt::json->>'amount')::json->>'unit' as unit
	,null as stage
	,null as attr
from raw_data.beers b
join json_array_elements(b.ingredients::json->'malt') as malt on TRUE
join core.dim_beer db on db.beer_id = b.id
join core.dim_ingredients di  on di.ingredient_name = malt::json->>'name'
union
select 
	db.id as dim_beer_id
	,di.ingredient_id as dim_ingredient_id
	,null as amount
	,null as unit
	,null as stage
	,null as attr
from raw_data.beers b
join core.dim_beer db on db.beer_id = b.id
join core.dim_ingredients di  on di.ingredient_name = b.ingredients::json->>'yeast'
order by 2,3
;