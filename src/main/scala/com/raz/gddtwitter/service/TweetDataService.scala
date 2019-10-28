package com.raz.gddtwitter.service

import java.sql.Timestamp

import com.raz.gddtwitter.config.properties.AppProperties
import com.raz.gddtwitter.service.SchemaConstants._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class TweetDataService @Autowired()(private val sparkSession: SparkSession,
                                    private val hdfsDataService: HdfsDataService,
                                    private val appProperties: AppProperties) extends Serializable {

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