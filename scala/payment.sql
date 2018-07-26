create table "public"."payment"(
	customer_id varchar(25) not null, 
	realm_id int not null, 
	town varchar(100), 
	country char(2) , 
	ts timestamp not null,
	payment_provider varchar(25),
	amount_with_tax float,
	currency char(3),
    foreign key(realm_id) references realm(realm_id),
    foreign key(country) references country(alpha_2)
) 
	COMPOUND SORTKEY(ts, realm_id, customer_id, country);
