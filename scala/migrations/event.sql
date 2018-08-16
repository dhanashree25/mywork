CREATE TABLE "public"."event_start" (
    "event_id" INT NOT NULL PRIMARY KEY,
    "realm_id" INT NOT NULL,
    "start_at" TIMESTAMP NOT NULL,
    FOREIGN KEY(realm_id) REFERENCES realm(realm_id)
) COMPOUND SORTKEY("start_at");

CREATE TABLE "public"."event_finish" (
    "event_id" INT NOT NULL PRIMARY KEY,
    "realm_id" INT NOT NULL,
    "finish_at" TIMESTAMP NOT NULL,
    FOREIGN KEY(realm_id) REFERENCES realm(realm_id)
) COMPOUND SORTKEY("finish_at");

CREATE TABLE "public"."event" (
    "event_id" INT NOT NULL PRIMARY KEY,
    "realm_id" INT NOT NULL,
    "start_at" TIMESTAMP NOT NULL,
    "finish_at" TIMESTAMP NOT NULL,
    "duration" INT NOT NULL,
    FOREIGN KEY(realm_id) REFERENCES realm(realm_id)
) COMPOUND SORTKEY("realm_id", "event_id", "start_at", "finish_at", "duration");


