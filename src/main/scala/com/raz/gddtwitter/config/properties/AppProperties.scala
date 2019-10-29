package com.raz.gddtwitter.config.properties

case class AppProperties(@transient sparkMaster: String,
                         @transient sparkShufflePartitions: String,
                         @transient hdfsName: String,
                         @transient sampleHdfsPath: String,
                         @transient twitterSampleHdfsPath: String) extends Serializable {

}
