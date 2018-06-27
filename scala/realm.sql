CREATE TABLE "public"."realm" (
    "realm_id" INT NOT NULL PRIMARY KEY,
    "name" VARCHAR(256) NOT NULL UNIQUE
) COMPOUND SORTKEY(realm_id, name);

INSERT INTO "realm" ("realm_id", "name") VALUES
    (2, 'dce.ufc'),
    (5, 'Unknown'),
    (6, 'Unknown'),
    (7, 'dce.epl'),
    (9, 'dce.pbr'),
    (10, 'dce.sport'),
    (11, 'dce.ssport'),
    (12, 'Unknown'),
    (13, 'Unknown'),
    (14, 'Unknown'),
    (15, 'dce.fivb'),
    (16, 'dce.seriea');

