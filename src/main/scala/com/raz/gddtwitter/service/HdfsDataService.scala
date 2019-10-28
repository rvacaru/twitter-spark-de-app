package com.raz.gddtwitter.service

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class HdfsDataService @Autowired()(private val sparkSession: SparkSession) extends Serializable {

  def retrieveSampleDataset(hdfsPath: String): Dataset[String] = {
    import sparkSession.implicits._

    val sampleExists: Boolean = existsInHdfs(hdfsPath)

    var sampleDataset = sparkSession.emptyDataset[String]
    if (sampleExists) {
      sampleDataset = sparkSession.read.textFile(hdfsPath)
    }

    sampleDataset
  }

  private def existsInHdfs(hdfsPath: String): Boolean = {
    val hadoopConfig = sparkSession.sparkContext.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConfig)
    val exists = hdfs.exists(new Path(hdfsPath))

    exists
  }

}
