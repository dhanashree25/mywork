CREATE TABLE "public"."realm" (
    "realm_id" INT NOT NULL UNIQUE PRIMARY KEY,
    "name" VARCHAR(256) NOT NULL UNIQUE
) COMPOUND SORTKEY("realm_id", "name");
