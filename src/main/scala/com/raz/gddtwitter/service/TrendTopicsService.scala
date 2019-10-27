package com.raz.gddtwitter.service

import java.util

import com.raz.gddtwitter.domain.{TopicsWindow, TrendingTopicsWindow, TrendingTopicsWindowApi}
import com.raz.gddtwitter.service.SchemaConstants._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class TrendTopicsService @Autowired()(private val dataProviderService: DataProviderService,
                                      private val sparkSession: SparkSession) extends Serializable {

  import sparkSession.implicits._

  private val MINUTE_DURATION = " minute"
  private val TOPICS = "topics"
  private val WINDOW_START = "window.start"
  private val WINDOW_END = "window.end"
  private val START = "start"
  private val END = "end"
  private val DATE_FORMAT_API = "yyyy-MM-dd HH:mm:ss"

//  @PostConstruct
//  def initTesting = {
////    getTopTrendingTopicsPerWindow(6, 60)
//  }

  def getTopTrendingTopicsPerWindowAsList(noTopTopics: Int, minutesWindowSize: Long): util.List[TrendingTopicsWindowApi] = {
    getTopTrendingTopicsPerWindow(noTopTopics, minutesWindowSize)
      .select(date_format(col(START), DATE_FORMAT_API).as(START), date_format(col(END), DATE_FORMAT_API).as(END), col(TOPICS))
      .as[TrendingTopicsWindowApi]
      .collectAsList()
  }

  def getTopTrendingTopicsPerWindow(noTopTopics: Int, minutesWindowSize: Long): Dataset[TrendingTopicsWindow] = {
    val topicsDf = getTopicsDf()
    val trendDf = groupTopicsPerWindow(topicsDf, minutesWindowSize)

    mapToTrendingTopicsPerWindow(trendDf, noTopTopics)
  }

  private def mapToTrendingTopicsPerWindow(trendDf: DataFrame, noTopTopics: Int): Dataset[TrendingTopicsWindow] = {
    val trendingTopicsDf = trendDf
      .select(col(WINDOW_START), col(WINDOW_END), col(TOPICS)).as[TopicsWindow]
      .map(tw => TrendingTopicsWindow(tw.start, tw.end,
        tw.topics.groupBy(identity).mapValues(_.size).toSeq.sortWith(_._2 > _._2).take(noTopTopics)))
      .sort(desc(START))

   trendingTopicsDf
  }

  private def groupTopicsPerWindow(topicsDf: DataFrame, minutesWindowSize: Long): DataFrame = {
    val trendDf = topicsDf
      .groupBy(window(col(CREATED_AT), minutesWindowSize + MINUTE_DURATION))
      .agg(collect_list(TOPIC).as(TOPICS))

    trendDf
  }

  private def getTopicsDf(): DataFrame = {
    dataProviderService
      .getTwitterTextDf()
      .cache()
      .withColumn(TEXT, explode(split(col(TEXT), "\\s+")))
      .withColumn(TEXT, lower(col(TEXT)))
      .withColumnRenamed(TEXT, TOPIC)
      .cache()
  }
}
