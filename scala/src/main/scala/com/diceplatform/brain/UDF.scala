package com.diceplatform.brain

import org.apache.spark.sql.functions.udf

object UDF {
  /**
    * A user defined function which converts an action integer to action string
    *
    * @todo Use Action object constants and when().otherwise() function
    */
  val actionIntToString = udf((action: Int) => action match {
    case 1  => "SESSION_CHECK"
    case 2  => "VOD_PROGRESS"
    case 3  => "LIVE_WATCHING"
    case 4  => "VOD_ACCESS_CHECK"
    case 5  => "LIVE_ACCESS_CHECK"
    case 6  => "USER_SIGN_IN"
    case 7  => "USER_SIGN_UP"
    case 8  => "HEALTH_CHECK"
    case -1 => "OTHER"
  })
}
