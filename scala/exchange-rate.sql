BEGIN TRANSACTION READ WRITE;

DROP TABLE IF EXISTS "public"."exchange_rate";

CREATE TABLE "public"."exchange_rate" (
    "ts" TIMESTAMP NOT NULL,
    "currency" VARCHAR(128) NOT NULL,
    "rate" TIMESTAMP NOT NULL,
    PRIMARY KEY(ts, currency)
) COMPOUND SORTKEY("ts", "currency", "rate");

COMMIT;
