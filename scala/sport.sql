CREATE TABLE "public"."sport" (
    "sport_id" INT NOT NULL UNIQUE PRIMARY KEY,
    "name" VARCHAR(256) NOT NULL UNIQUE
) COMPOUND SORTKEY("sport_id", "name");
