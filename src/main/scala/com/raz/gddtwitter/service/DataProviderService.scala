package com.raz.gddtwitter.service

import java.sql.Timestamp

import com.raz.gddtwitter.config.properties.AppProperties
import com.raz.gddtwitter.service.SchemaConstants._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class DataProviderService @Autowired()(private val sparkSession: SparkSession,
                                       private val appProperties: AppProperties) extends Serializable {

  def getTwitterTextDf(): DataFrame = {
    import sparkSession.implicits._

    val sampleExists: Boolean = existsInHdfs(appProperties.sampleHdfsPath)
    val twitterSampleExists: Boolean = existsInHdfs(appProperties.twitterSampleHdfsPath)

    var filteredSampleDataset = sparkSession.emptyDataset[String]
    if (sampleExists) {
      filteredSampleDataset = readAndFilterSampleJson(appProperties.sampleHdfsPath,
        (str: String) => str.startsWith(COORDINATES_PREFIX))
    }

    var filteredTwitterSampleDataset = sparkSession.emptyDataset[String]
    if (twitterSampleExists) {
      filteredTwitterSampleDataset = readAndFilterSampleJson(appProperties.twitterSampleHdfsPath,
        (str: String) => str.contains(TWITTER_TYPE_PATTERN) && !str.contains(RETWEET_PATTERN))
    }

    toTwitterCreatedAtAndTextDf(filteredTwitterSampleDataset)
      .union(toCreatedAtAndTextDf(filteredSampleDataset))
  }

  private def existsInHdfs(hdfsPath: String): Boolean = {
    val hadoopConfig = sparkSession.sparkContext.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConfig)
    val exists = hdfs.exists(new Path(hdfsPath))

    exists
  }

  private def readAndFilterSampleJson(hdfsPath: String, filterFunction: String => Boolean): Dataset[String] = {
    sparkSession.read
      .textFile(hdfsPath)
      .filter(filterFunction)
  }

  private def toCreatedAtAndTextDf(stringDataset: Dataset[String]): DataFrame = {
    if (stringDataset.isEmpty) {
      return emptyTwitterDf()
    }

    val filterDf = sparkSession.read.json(stringDataset)
      .select(col(CREATED_AT), col(TEXT))
      .where(createdAtAndTextAreNotNullOrEmptyStringsCondition())
      .withColumn(CREATED_AT, to_timestamp(col(CREATED_AT), DATE_FORMAT_SAMPLE))

    filterDf
  }

  private def toTwitterCreatedAtAndTextDf(stringDataset: Dataset[String]): DataFrame = {
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
    col(CREATED_AT).isNotNull && col(CREATED_AT) =!= "" && col(TEXT).isNotNull && col(TEXT) =!= ""
  }

  private def emptyTwitterDf(): DataFrame = {
    import sparkSession.implicits._
    Seq.empty[(Timestamp, String)].toDF(Seq(CREATED_AT, TEXT): _*)
  }

}