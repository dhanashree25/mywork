CREATE TABLE "public"."sport" (
    "sport_id" INT NOT NULL UNIQUE PRIMARY KEY,
    "name" VARCHAR(256) NOT NULL UNIQUE
) COMPOUND SORTKEY("sport_id", "name");

INSERT INTO "sport" ("sport_id", "name") VALUES
    (3, 'Test'),
    (7, 'Football'),
    (11, 'NBA'),
    (17, 'UFC'),
    (19, 'Mixed');
