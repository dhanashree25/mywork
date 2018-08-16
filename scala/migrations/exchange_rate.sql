BEGIN TRANSACTION READ WRITE;

DROP TABLE IF EXISTS "public"."exchange_rate";

CREATE TABLE "public"."exchange_rate" (
    "date" DATE NOT NULL,
    "currency" CHAR(3) NOT NULL,
    "rate" NUMERIC(12, 4) NOT NULL,
    PRIMARY KEY(date, currency)
) COMPOUND SORTKEY("date", "currency", "rate");

COMMIT;
