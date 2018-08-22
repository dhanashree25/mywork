CREATE TABLE "public"."live_catalogue" (
    "realm_id" INT NOT NULL,
    "title" VARCHAR(1024) NOT NULL,
    "event_id" INT NOT NULL,
    "sport_id" INT NOT NULL,
    "tournament_id" INT NOT NULL,
    "duration" INT NOT NULL,
    "thumbnail_url" VARCHAR(1024) NOT NULL,
    "deleted" BOOLEAN NOT NULL,
    "geo_restriction_type" CHAR(9) NOT NULL,
    "geo_restriction_countries" VARCHAR(2048) NOT NULL,
    "start_at" TIMESTAMP,
    "finish_at" TIMESTAMP,
    "updated_at" TIMESTAMP,
    FOREIGN KEY(realm_id) REFERENCES realm(realm_id)
) COMPOUND SORTKEY(realm_id, updated_at, deleted);