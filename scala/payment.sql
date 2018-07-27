create table "public"."test_payment"(
	customer_id varchar(25) not null, 
	realm_id int not null, 
	town varchar(100), 
	country char(2) , 
	ts timestamp not null,
	payment_provider varchar(25),
	amount_with_tax float,
	currency char(3),
	sku varchar(50),
	payment_id varchar(50),
    foreign key(realm_id) references realm(realm_id),
    foreign key(country) references country(alpha_2),
    foreign key(currency) references country(alphabetic_code)
) 
	COMPOUND SORTKEY(ts, realm_id, customer_id, country);

create table "public"."test_subscription"(
	customer_id varchar(25) not null, 
	realm_id int not null, 
	town varchar(100), 
	country char(2) , 
	ts timestamp not null,
	payment_id varchar(50),
	is_trial boolean,
	trial_days integer,
	sku varchar(50),
	revoked boolean,
	cancelled boolean,
    foreign key(realm_id) references realm(realm_id),
    foreign key(country) references country(alpha_2)
) 
	COMPOUND SORTKEY(realm_id, country);
	