package com.raz.gddtwitter.service

import java.sql.Timestamp

import com.raz.gddtwitter.domain.{TopicsWindow, TrendingTopicsWindow}
import com.raz.gddtwitter.service.SchemaConstants._
import javax.annotation.PostConstruct
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class TrendTopicsService @Autowired()(private val dataProviderService: DataProviderService, private val sparkSession: SparkSession) extends Serializable {

  private val MINUTE_DURATION = " minute"
  private val TOPICS = "topics"
  private val START = "start"

  @PostConstruct
  def initTesting = {
    getTopTrendingTopicsPerWindow(3, 60)
  }

  def getTopTrendingTopicsPerWindow(noTopTopics: Int, minutesWindowSize: Long) = {
    val textDf = dataProviderService.getTwitterTextDf.sort(CREATED_AT).cache()

    val topicsDf = getTopicsDf(textDf)

    val windowDuration: String = minutesWindowSize + MINUTE_DURATION

    val trendDf = topicsDf.groupBy(window(col(CREATED_AT), windowDuration))
        .agg(collect_list(TOPIC).as(TOPICS))

    import sparkSession.implicits._

    val trendingTopicsDf = trendDf
      .select("window.start", "window.end", TOPICS).as[TopicsWindow]
      .map(tw => TrendingTopicsWindow(tw.start, tw.end,
        tw.topics.groupBy(identity).mapValues(_.size).toSeq.sortWith(_._2 > _._2).take(noTopTopics))
      )

    val x = trendingTopicsDf.sort(desc(START))

    x
  }

  private def groupTopTrendingTopics(noTopTopics: Int, topics: Column): Column = {
    topics.getItem()
  }

  def getTopicsDf(textDf: DataFrame): DataFrame = {
    textDf
      .withColumn(TEXT, explode(split(col(TEXT), " ")))
      .withColumn(TEXT, lower(col(TEXT)))
      .withColumnRenamed(TEXT, TOPIC)
  }
}
