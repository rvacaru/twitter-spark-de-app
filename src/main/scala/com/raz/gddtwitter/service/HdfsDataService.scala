package com.raz.gddtwitter.service

import org.apache.spark.sql.{Dataset, SparkSession}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

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
