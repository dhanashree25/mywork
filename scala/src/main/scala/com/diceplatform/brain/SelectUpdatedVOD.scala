package com.diceplatform.brain

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, expr}

object SelectUpdatedVOD {
  def transform(df: DataFrame): Unit = {
        //                           Table "public.catalogue"
        //    Column     |            Type             | Collation | Nullable | Default
        //---------------+-----------------------------+-----------+----------+---------
        // video_id      | integer                     |           |          |
        // video_dve_id  | integer                     |           |          |
        // realm_id      | integer                     |           |          |
        // title         | character varying(256)      |           |          |
        // description   | character varying(2048)     |           |          |
        // duration      | integer                     |           |          |
        // thumbnail_url | character varying(1024)     |           |          |
        // deleted       | boolean                     |           |          |
        // draft         | boolean                     |           |          |
        // imported_at   | timestamp without time zone |           |          |
        // updated_at    | timestamp without time zone |           |          |

        df.where(expr(s"payload.data.ta == '${ActionType.UPDATED_VOD}'"))
          .select(
            expr("payload.video AS video_id"),
            expr("payload.data.v.vodDveId AS video_dve_id"),
            expr("realm AS realm_id"), // TODO Join
            col("payload.data.v.title"),
            col("payload.data.v.description"),
            col("payload.data.v.duration"),
            expr("payload.data.v.thumbnailUrl AS thumbnail_url"),
            col("payload.data.v.deleted"),
            col("payload.data.v.duration"),
            col("ts")
          )
          .write
          .format("jdbc")
          .mode(SaveMode.Overwrite)
          .option("url", "jdbc:postgresql://qa-redshift-cluster.camxxuuvbchc.eu-west-1.redshift.amazonaws.com/redshift?user=saffron&password=1Nn0v8t3")
          .option("driver", "com.amazon.redshift.jdbc.Driver")
          .option("dbtable", "test")
          .option("user", "saffron")
          .option("password", "1Nn0v8t3")
          .save()
  }
}