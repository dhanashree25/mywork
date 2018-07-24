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
	start_at timestamp,
	video_duration int not null,
	progress int not null,
	perc_drop float,
    foreign key(realm_id) references realm(realm_id),
    foreign key(video_id) references vod_catalogue(video_dve_id)
)
	COMPOUND SORTKEY(realm_id, video_id, customer_id, start_at);