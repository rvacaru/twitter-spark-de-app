package com.raz.gddtwitter.service

import java.sql.Timestamp

import com.raz.gddtwitter.config.properties.AppProperties
import com.raz.gddtwitter.service.SchemaConstants._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

/**
 * Service fetching two types of datasets (for now) which exposes to the upper services a dataframe of tweets
 * for further analyzes.
 * The service unifies the tweets coming from the two datasets into a single dataframe, if one of the initial datasets
 * is missing the upper services will analyze based on only part of the data available.
 *
 * If no data is available the choice of what to do next is left to the upper services. If desired an exception can be
 * thrown here when there is no data to analyze coming from hdfs.
 */
@Service
class TweetDataService @Autowired()(private val sparkSession: SparkSession,
                                    private val hdfsDataService: HdfsDataService,
                                    private val appProperties: AppProperties) extends Serializable {

  private val TWITTER_CREATED_AT: String = "twitter." + CREATED_AT
  private val TWITTER_TEXT: String = "twitter." + TEXT
  private val DATE_FORMAT_SAMPLE: String = "EEE MMM dd HH:mm:ss Z yyyy"
  private val DATE_FORMAT_TWITTER_SAMPLE: String ="EEE, dd MMM yyyy HH:mm:ss Z"
  private val COORDINATES_PREFIX: String = "{\"coordinates\":"
  private val TWITTER_TYPE_PATTERN: String = "\"type\":\"twitter\""
  private val RETWEET_PATTERN: String = "\"retweet\":{"

  def getTweetDf(): DataFrame = {

    val filteredSampleDataset: Dataset[String] = hdfsDataService
      .retrieveSampleDataset(appProperties.sampleHdfsPath)
      .filter(str => str.startsWith(COORDINATES_PREFIX))

    val filteredTwitterSampleDataset = hdfsDataService
      .retrieveSampleDataset(appProperties.twitterSampleHdfsPath)
      .filter(str => str.contains(TWITTER_TYPE_PATTERN) && !str.contains(RETWEET_PATTERN))

    twitterSampleToTweetDf(filteredTwitterSampleDataset)
      .union(sampleToTweetDf(filteredSampleDataset))
  }

  private def sampleToTweetDf(stringDataset: Dataset[String]): DataFrame = {
    if (stringDataset.isEmpty) {
      return emptyTwitterDf()
    }

    val filterDf = sparkSession.read.json(stringDataset)
      .select(col(CREATED_AT), col(TEXT))
      .where(createdAtAndTextAreNotNullOrEmptyStringsCondition())
      .withColumn(CREATED_AT, to_timestamp(col(CREATED_AT), DATE_FORMAT_SAMPLE))

    filterDf
  }

  private def twitterSampleToTweetDf(stringDataset: Dataset[String]): DataFrame = {
    if (stringDataset.isEmpty) {
      return emptyTwitterDf()
    }

    val filterDf = sparkSession.read.json(stringDataset)
      .select(col(TWITTER_CREATED_AT), col(TWITTER_TEXT))
      .where(createdAtAndTextAreNotNullOrEmptyStringsCondition())
      .withColumn(CREATED_AT, to_timestamp(col(CREATED_AT), DATE_FORMAT_TWITTER_SAMPLE))

    filterDf
  }

  private def createdAtAndTextAreNotNullOrEmptyStringsCondition(): Column = {
    col(CREATED_AT).isNotNull && col(CREATED_AT) =!= "" &&
      col(TEXT).isNotNull && col(TEXT) =!= ""
  }

  private def emptyTwitterDf(): DataFrame = {
    import sparkSession.implicits._
    Seq.empty[(Timestamp, String)].toDF(Seq(CREATED_AT, TEXT): _*)
  }

}