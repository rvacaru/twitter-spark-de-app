package com.raz.gddtwitter.service

import org.apache.spark.sql.{Dataset, SparkSession}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

/**
 * Service exposing any hdfs data file as a dataset of strings to any upper services.
 * This services checks if the data file exists and can return an empty dataset if the hdfs data file is missing.
 */
@Service
class HdfsDataService @Autowired()(private val sparkSession: SparkSession,
                                   private val hdfsUtilService: HdfsUtilService) extends Serializable {

  def retrieveSampleDataset(hdfsPath: String): Dataset[String] = {
    import sparkSession.implicits._

    val sampleExists: Boolean = hdfsUtilService.existsInHdfs(hdfsPath)

    var sampleDataset = sparkSession.emptyDataset[String]
    if (sampleExists) {
      sampleDataset = sparkSession.read.textFile(hdfsPath)
    }

    sampleDataset
  }

}
