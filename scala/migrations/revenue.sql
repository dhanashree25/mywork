BEGIN TRANSACTION READ WRITE;

DROP TABLE IF EXISTS "public"."revenue_hourly";
CREATE TABLE "public"."revenue_hourly" (
    "realm_id" INT NOT NULL,
    "payment_provider" VARCHAR(16) NOT NULL,
    "currency" VARCHAR(4) NOT NULL,
    "amount" INT NOT NULL,
    "amount_usd" INT NOT NULL,
    "ts" TIMESTAMP,
    FOREIGN KEY(realm_id) REFERENCES realm(realm_id)
) COMPOUND SORTKEY(ts, realm_id, payment_provider, currency);

DROP TABLE IF EXISTS "public"."revenue_daily";
CREATE TABLE "public"."revenue_daily" (
    "realm_id" INT NOT NULL,
    "payment_provider" VARCHAR(16) NOT NULL,
    "currency" VARCHAR(4) NOT NULL,
    "amount" INT NOT NULL,
    "amount_usd" INT NOT NULL,
    "ts" TIMESTAMP,
    FOREIGN KEY(realm_id) REFERENCES realm(realm_id)
) COMPOUND SORTKEY(ts, realm_id, payment_provider, currency);

DROP TABLE IF EXISTS "public"."revenue_weekly";
CREATE TABLE "public"."revenue_weekly" (
    "realm_id" INT NOT NULL,
    "payment_provider" VARCHAR(16) NOT NULL,
    "currency" VARCHAR(4) NOT NULL,
    "amount" INT NOT NULL,
    "amount_usd" INT NOT NULL,
    "ts" TIMESTAMP,
    FOREIGN KEY(realm_id) REFERENCES realm(realm_id)
) COMPOUND SORTKEY(ts, realm_id, payment_provider, currency);

DROP TABLE IF EXISTS "public"."revenue_monthly";
CREATE TABLE "public"."revenue_monthly" (
    "realm_id" INT NOT NULL,
    "payment_provider" VARCHAR(16) NOT NULL,
    "currency" VARCHAR(4) NOT NULL,
    "amount" INT NOT NULL,
    "amount_usd" INT NOT NULL,
    "ts" TIMESTAMP,
    FOREIGN KEY(realm_id) REFERENCES realm(realm_id)
) COMPOUND SORTKEY(ts, realm_id, payment_provider, currency);

COMMIT;
