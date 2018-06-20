CREATE TABLE "public"."realm" (
    "realm_id" INT NOT NULL PRIMARY KEY,
    "name" VARCHAR(256) NOT NULL UNIQUE
) COMPOUND SORTKEY(realm_id, name);

INSERT INTO "realm" ("realm_id", "name") VALUES
    (2, 'Test'),
    (5, 'Test'),
    (6, 'Test'),
    (7, 'Premier League'),
    (9, 'PBR'),
    (10, 'Test'),
    (11, 'Test'),
    (12, 'Food Tube'),
    (13, 'Test'),
    (14, 'WWE'),
    (15, 'Volleyball'),
    (16, 'Test');
