package com.raz.gddtwitter.service

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class HdfsUtilService @Autowired()(private val sparkSession: SparkSession) extends Serializable {

  def existsInHdfs(hdfsPath: String): Boolean = {
    val hadoopConfig = sparkSession.sparkContext.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConfig)
    val exists = hdfs.exists(new Path(hdfsPath))

    exists
  }

}
