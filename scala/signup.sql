create table "public"."signups"(
	customer_id varchar(25) not null, 
	realm_id int not null, 
	town varchar(100), 
	country char(2) , 
	ts timestamp not null,
	device varchar(25),
    foreign key(realm_id) references realm(realm_id),
    foreign key(country) references country(alpha_2)
	) 
	COMPOUND SORTKEY(ts, realm_id, device, country);
