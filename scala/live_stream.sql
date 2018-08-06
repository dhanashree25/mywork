CREATE TABLE "public"."live_stream" (
    "realm_id" INT NOT NULL,
    "title" VARCHAR(1024) NOT NULL,
    "event_id" INT NOT NULL,
    "sport_id" INT NOT NULL,
    "tournament_id" INT NULL,
    "property_id" INT NULL, -- Unknown
    "duration" INT NOT NULL,
    "thumbnail_url" VARCHAR(1024) NOT NULL,
    "deleted" BOOLEAN NOT NULL,
    "geo_restriction_type" CHAR(9) NOT NULL, -- WHITELIST or BLACKLIST
    "geo_restriction_countries" VARCHAR(2048) NOT NULL, -- There are 676 2 letter codes, 2 char each + delimiter ~2048
    "start_at" TIMESTAMP NOT NULL,
    "finish_at" TIMESTAMP NOT NULL,
    "updated_at" TIMESTAMP NOT NULL,
    FOREIGN KEY(realm_id) REFERENCES realm(realm_id)
) COMPOUND SORTKEY(realm_id, event_id);
