create table "public"."signups"(
	customer_id varchar(25), 
	realm varchar(50), 
	town varchar(100), 
	country varchar(100) , 
	ts timestamp ,
	device varchar(25)
	) 
	COMPOUND SORTKEY(ts, realm, device, country);
