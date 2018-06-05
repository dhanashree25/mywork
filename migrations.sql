create table dce_events(session_id varchar(50),
client_ip varchar(25),
country varchar(25),
customer_id varchar(25),
device varchar(20),
progress int,
realm varchar(20),
started_at timestamp,
ta varchar(10),
town varchar(100),
ts timestamp,
video_id int,
action int);

create table dce_video_plays(
session_id varchar(50),
customer_id varchar(25),
video_id integer,
duration integer,
started_at timestamp,
start_time timestamp,
end_time timestamp,
country varchar(25),
town varchar(100),
realm varchar(25), 
latitude float, 
longitude float);

create table signups(customer_id varchar(20), realm varchar(50), town varchar(50), country varchar(50) , ts timestamp ,device varchar(20)) COMPOUND SORTKEY(ts, realm, device, country);