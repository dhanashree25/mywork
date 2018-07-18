                    Table "public.video_dropouts"
   Column    |            Type             | Collation | Nullable | Default 
-------------+-----------------------------+-----------+----------+---------
 video_id    | integer                     |           |          | 
 customer_id | character varying(25)       |           |          | 
 start_time  | timestamp without time zone |           |          | 
 year        | double precision            |           |          | 
 month       | double precision            |           |          | 
 day         | double precision            |           |          | 
 dow         | double precision            |           |          | 
 hour        | double precision            |           |          | 
 progress    | integer                     |           |          | 
 duration    | integer                     |           |          | 
 perc_drop   | integer                     |           |          | 
 
create table vod_dropoff(
	realm_id integer,
	video_id integer,
	customer_id varchar(32) not null,
	session_id varchar(32),
    year int,
    month int,
    day int,
    dow int,
    hour int,
	started_at timestamp,
	video_duration int not null,
	progress int not null,
	perc_drop float,
    foreign key(realm_id) references realm(realm_id)
	)
	COMPOUND SORTKEY(realm_id, video_id, customer_id);