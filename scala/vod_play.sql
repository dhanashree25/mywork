 create table vod_play(
    realm_id int not null,
    session_id varchar(64)  not null,
    customer_id varchar(32) not null,
    video_id integer,
    duration int not null,
    started_at timestamp,
    start_at timestamp,
    end_at timestamp,
    country char(2),
    town varchar(256),
    foreign key(realm_id) references realm(realm_id),
    foreign key(country) references country(alpha_2)
) COMPOUND SORTKEY(realm_id, session_id, customer_id, video_id);
