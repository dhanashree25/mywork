create table user_logins(
	customer_id varchar(25),
    realm varchar(50),
    town varchar(100),
    country varchar(100),
    client_ip varchar(50),
    type varchar(50),
    device varchar(25),
    ts timestamp
    )
    COMPOUND SORTKEY(ts, realm, device, country);