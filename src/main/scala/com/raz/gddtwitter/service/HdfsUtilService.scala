package com.raz.gddtwitter.service

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

/**
 * The service provides auxiliary functionalities related to sample files stored in hdfs
 */
@Service
class HdfsUtilService @Autowired()(private val sparkSession: SparkSession) extends Serializable {

  /**
   * Function to be used before accessing a data file, business logic should first check if the path cofigured
   * points to a real file and handle missing data files gracefully.
   *
   * @param hdfsPath path of the sample data file
   * @return a boolean telling if the data file exists at the given path or not
   */
  def existsInHdfs(hdfsPath: String): Boolean = {
    val hadoopConfig = sparkSession.sparkContext.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConfig)
    val exists = hdfs.exists(new Path(hdfsPath))

    exists
  }

}
