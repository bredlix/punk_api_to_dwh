CREATE SCHEMA RAW_DATA;
CREATE SCHEMA CORE;
CREATE SCHEMA DATA_MART;

create table if not exists core.dim_beer (
    id serial primary key,
    beer_id int,
    beer_name varchar,
    beer_tagline varchar,
    first_brewed_year int,
    description varchar,
    image_url varchar,
    abv decimal,
    ibu decimal,
    target_fg decimal,
    ebc decimal,
    srm decimal,
    attenuation_level decimal,
    volume_litres decimal,
    boil_volume_litres decimal,
    brewers_tips varchar
    );

create table core.dim_ingredients (
	ingredient_id serial primary key,
	ingredient_type varchar,
	ingredient_name varchar
	);

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
	);

create table core.link_method_beer(
	id serial primary key,
	dim_beer_id int,
	stage varchar,
	temp decimal,
	temp_unit varchar,
	duration decimal,
	foreign key (dim_beer_id) references core.dim_beer (id)
    );