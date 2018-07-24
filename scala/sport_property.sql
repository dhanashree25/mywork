CREATE TABLE "public"."sport_property" (
    "sport_id" INT NOT NULL REFERENCES sport(sport_id),
    "property_id" INT NOT NULL PRIMARY KEY,
    "name" VARCHAR(256) NOT NULL,
    UNIQUE (sport_id, property_id, name)
) COMPOUND SORTKEY("sport_id", "property_id", "name");
