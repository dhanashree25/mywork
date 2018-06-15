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

create table video_plays(
session_id varchar(50),
customer_id varchar(25),
video_id integer,
duration integer,
started_at timestamp,
start_time timestamp,
end_time timestamp,
country varchar(100),
town varchar(100),
realm varchar(25))
COMPOUND SORTKEY(realm, video_id, start_time, end_time,customer_id, country);

create table video_dropoffs(
session_id varchar(50),
customer_id varchar(25),
video_id integer,
start_time timestamp,
end_time timestamp,
perc_viewed float,
country varchar(100),
town varchar(100),
realm varchar(25))
COMPOUND SORTKEY(realm, video_id, start_time, end_time,customer_id, country);

create table signups(customer_id varchar(20), realm varchar(50), town varchar(50), country varchar(50) , ts timestamp ,device varchar(20)) COMPOUND SORTKEY(ts, realm, device, country);

create table user_logins(customer_id varchar(25),
			realm varchar(50),
			town varchar(100),
			country varchar(100),
			client_ip varchar(50),
			ta varchar(50),
			ts timestamp)
			COMPOUND SORTKEY(ts, realm, ta, country);
			
create table video_dropouts as select v.video_id ,
 v.customer_id ,v.start_time, 
 date_part (y, start_time)as year, 
 date_part (mon, start_time)as month, 
 date_part (day, start_time)as day, 
 date_part (dow, start_time)as dow, 
 date_part (h, start_time)as hour, 
 v.progress, c.duration, 100*v.progress/c.duration as perc_drop 
 from (select session_id, customer_id, realm, video_id, town, country, min(start_time) start_time,max(end_time) end_time, max(duration) progress from
  video_plays group by session_id, customer_id, realm, video_id, town, country)as v join catalogue c on v.video_id= c.video_dve_id order by v.video_id, v.customer_id;

