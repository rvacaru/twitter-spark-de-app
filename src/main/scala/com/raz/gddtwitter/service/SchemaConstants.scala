package com.raz.gddtwitter.service

object SchemaConstants {

  val CREATED_AT: String = "created_at"
  val TEXT: String = "text"
  val TWITTER_CREATED_AT: String = "twitter." + CREATED_AT
  val TWITTER_TEXT: String = "twitter." + TEXT
  val DATE_FORMAT_SAMPLE: String = "EEE MMM dd HH:mm:ss Z yyyy"
  val DATE_FORMAT_TWITTER_SAMPLE: String ="EEE, dd MMM yyyy HH:mm:ss Z"
  val COORDINATES_PREFIX: String = "{\"coordinates\":"
  val TWITTER_TYPE_PATTERN: String = "\"type\":\"twitter\""
  val RETWEET_PATTERN: String = "\"retweet\":{"

  val TOPIC: String = "topic"

}
