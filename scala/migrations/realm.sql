BEGIN TRANSACTION READ WRITE;

DROP TABLE IF EXISTS "public"."realm";

CREATE TABLE "public"."realm" (
    "realm_id" INT NOT NULL UNIQUE PRIMARY KEY,
    "name" VARCHAR(256) NOT NULL UNIQUE
) COMPOUND SORTKEY("realm_id", "name");

INSERT INTO "public"."realm" ("realm_id", "name") VALUES
  (1, 'dce.ufc'),
  (2, 'test-dve'),
  (3, 'prod-dve'),
  (4, 'dev-dve'),
  (5, 'sampaoli'),
  (7, 'dce.ent'),
  (8, 'dce.sport'),
  (9, 'dce.epl'),
  (10, 'dce.pbr'),
  (11, 'dce.ash'),
  (12, 'dce.ssport'),
  (13, 'dce.jo'),
  (14, 'dce.sandbox'),
  (15, 'devops'),
  (16, 'dce.wwe'),
  (17, 'dce.fivb'),
  (18, 'dce.seriea'),
  (19, 'dce.imgott');
COMMIT;
