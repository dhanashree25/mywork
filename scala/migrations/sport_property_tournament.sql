CREATE TABLE "public"."sport_property_tournament" (
    "sport_id" INT NOT NULL REFERENCES sport(sport_id),
    "property_id" INT NOT NULL REFERENCES sport_property(property_id),
    "tournament_id" INT NOT NULL UNIQUE PRIMARY KEY,
    "name" VARCHAR(256) NOT NULL UNIQUE
) COMPOUND SORTKEY("sport_id", "property_id", "tournament_id", "name");
