package com.raz.gddtwitter.service

import com.raz.gddtwitter.domain.{TopicsWindow, TrendingTopicsWindow, TrendingTopicsWindowApi}
import com.raz.gddtwitter.service.SchemaConstants._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class TrendTopicsService @Autowired()(private val sparkSession: SparkSession,
                                      private val tweetDataService: TweetDataService) extends Serializable {

  import sparkSession.implicits._

  private val VALID_WINDOW_INTERVALS = Seq("second", "minute", "hour", "day", "week")
  private val TOPICS = "topics"
  private val WINDOW_START = "window.start"
  private val WINDOW_END = "window.end"
  private val START = "start"
  private val END = "end"
  private val DATE_FORMAT_API = "yyyy-MM-dd HH:mm:ss"

  def getTopTrendingTopicsPerWindowAsSet(noTopTopics: Int, windowPhrase: String): Set[TrendingTopicsWindowApi] = {
    validateWindowPhrase(windowPhrase)
    val trendingTopicsList = getTopTrendingTopicsPerWindow(noTopTopics, windowPhrase)
      .select(date_format(col(START), DATE_FORMAT_API).as(START), date_format(col(END), DATE_FORMAT_API).as(END), col(TOPICS))
      .as[TrendingTopicsWindowApi]
      .collect()
      .toSet

    trendingTopicsList
  }

  def getTopTrendingTopicsPerWindow(noTopTopics: Int, windowPhrase: String): Dataset[TrendingTopicsWindow] = {
    val topicsDf = getTopicsDf()
    val topicsPerWindowDf = groupTopicsPerWindow(topicsDf, windowPhrase)

    mapToTrendingTopicsWindow(topicsPerWindowDf, noTopTopics)
  }

  private def mapToTrendingTopicsWindow(topicsPerWindowDf: DataFrame, noTopTopics: Int): Dataset[TrendingTopicsWindow] = {
    val trendingTopicsWindowDf = topicsPerWindowDf
      .select(col(WINDOW_START), col(WINDOW_END), col(TOPICS)).as[TopicsWindow]
      .map(tw => TrendingTopicsWindow(tw.start, tw.end,
        tw.topics.groupBy(identity).mapValues(_.size).toSeq.sortWith(_._2 > _._2).take(noTopTopics)))
      .sort(desc(START))

   trendingTopicsWindowDf
  }

  private def groupTopicsPerWindow(topicsDf: DataFrame, windowPhrase: String): DataFrame = {
    val topicsPerWindowDf = topicsDf
      .groupBy(window(col(CREATED_AT), windowPhrase))
      .agg(collect_list(TOPIC).as(TOPICS))

    topicsPerWindowDf
  }

  private def getTopicsDf(): DataFrame = {
    tweetDataService
      .getTweetDf()
      .withColumn(TEXT, explode(split(col(TEXT), "\\s+")))
      .withColumn(TEXT, lower(col(TEXT)))
      .withColumnRenamed(TEXT, TOPIC)
  }

  private def validateWindowPhrase(windowPhrase: String): Unit = {
    val tokens = windowPhrase.split(" ")
    tokens(0).toInt
    if (!VALID_WINDOW_INTERVALS.contains(tokens(1))) {
      val ex = new IllegalArgumentException("Window phrase is invalid")
      throw ex
    }
  }

}
