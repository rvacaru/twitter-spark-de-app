package com.raz.gddtwitter.config

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.springframework.context.annotation.{Bean, Configuration}

@Configuration
class SparkConfig {

  @Bean
  def sparkSession(): SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf())
      .getOrCreate()
  }

  @Bean
  def sparkContext(sparkSession: SparkSession): SparkContext = {
    sparkSession.sparkContext
  }

  private def sparkConf(): SparkConf = {
    new SparkConf()
      .setAppName("Spark Twitter App")
      .set("spark.master", "local[*]")

    //eventually more configuration
  }
}