package com.raz.gddtwitter.config

import com.raz.gddtwitter.config.properties.AppProperties
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.{Bean, Configuration}

@Configuration
class PropertiesConfig {

  @Value("${spark.master}")
  private val sparkMaster: String = null

  @Value("${spark.sql.shuffle.partitions:200}")
  private val sparkShufflePartitions: String = null

  @Value("${hdfs.default.name}")
  private val hdfsName: String = null

  @Value("${topics.hdfs.path.sample:hdfs:///default/path/sample.json}")
  private val sampleHdfsPath: String = null

  @Value("${topics.hdfs.path.twitter-sample:hdfs:///default/path/twitter-sample.json}")
  private val twitterSampleHdfsPath: String = null

  @Bean
  def appProperties: AppProperties = {
    AppProperties(
      sparkMaster,
      sparkShufflePartitions,
      hdfsName,
      sampleHdfsPath,
      twitterSampleHdfsPath
    )
  }

}
