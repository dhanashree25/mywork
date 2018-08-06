create table user_logins(
    customer_id varchar(25) not null,
    realm_id int not null,
    town varchar(100),
    country char(2),
    client_ip varchar(50),
    type varchar(50),
    device varchar(25),
    ts timestamp not null,
    foreign key(realm_id) references realm(realm_id),
    foreign key(country) references country(alpha_2)
)
COMPOUND SORTKEY(ts, realm_id, device, country);

alter table user_logins
add column is_success boolean; // Todo- make it not null when rerunning whole data
