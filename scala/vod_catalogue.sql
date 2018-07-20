CREATE TABLE "public"."vod_catalogue" (
    "video_id" INT NOT NULL,
    "video_dve_id" INT NOT NULL,
    "realm_id" INT NOT NULL,
    "title" VARCHAR(1024) NOT NULL,
    "description" VARCHAR(4096) NOT NULL,
    "duration" INT NOT NULL,
    "thumbnail_url" VARCHAR(1024) NOT NULL,
    "deleted" BOOLEAN NOT NULL,
    "draft" BOOLEAN NOT NULL,
    "tags" VARCHAR(1024),
    "imported_at" TIMESTAMP,
    "updated_at" TIMESTAMP
) COMPOUND SORTKEY(realm_id, updated_at, deleted, draft);
