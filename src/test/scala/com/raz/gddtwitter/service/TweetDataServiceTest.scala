package com.raz.gddtwitter.service

import java.sql.Timestamp

import com.holdenkarau.spark.testing.{DatasetSuiteBase, SharedSparkContext}
import com.raz.gddtwitter.config.properties.AppProperties
import com.raz.gddtwitter.service.SchemaConstants.{CREATED_AT, TEXT}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.junit.runner.RunWith
import org.mockito.Mockito.{mock, when, withSettings}
import org.scalatest.check.Checkers
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, PrivateMethodTester}


@RunWith(classOf[JUnitRunner])
class TweetDataServiceTest extends FlatSpec with PrivateMethodTester
  with DatasetSuiteBase with SharedSparkContext with Checkers  {

  private val VALID_SAMPLE_PATH: String = getClass.getResource("/testSampleValid.json").getPath()
  private val VALID_TWITTER_SAMPLE_PATH: String = getClass.getResource("/testTwitterValid.json").getPath()
  private val INVALID_TWITTER_SAMPLE_PATH: String = getClass.getResource("/testTwitterInvalidFields.json").getPath()

  private val INVALID_LINES_SAMPLE_PATH: String = getClass.getResource("/testSampleInvalidLines.json").getPath()
  private val INVALID_LINES_TWITTER_SAMPLE_PATH: String = getClass.getResource("/testTwitterInvalidLines.json").getPath()


  private trait Test {
    val hdfsDataServiceMock: HdfsDataService = mock(classOf[HdfsDataService], withSettings().serializable())
    val appProperties: AppProperties =
      AppProperties("testMaster", "200", "testHdfsName", VALID_SAMPLE_PATH, VALID_TWITTER_SAMPLE_PATH)

    val tweetDataService: TweetDataService = new TweetDataService(spark, hdfsDataServiceMock, appProperties)
  }

  "getTweetDf" should "union the two datasets into one dataframe" in new Test {
    private val validSampleDs = createStringDs(VALID_SAMPLE_PATH)
    private val validTwitterSamepleDs = createStringDs(VALID_TWITTER_SAMPLE_PATH)
    when(hdfsDataServiceMock.retrieveSampleDataset(VALID_SAMPLE_PATH)).thenReturn(validSampleDs)
    when(hdfsDataServiceMock.retrieveSampleDataset(VALID_TWITTER_SAMPLE_PATH)).thenReturn(validTwitterSamepleDs)

    private val actualTweetDf = tweetDataService.getTweetDf()
//    assertDataFrameEquals(expectedDf, actualDf) //sad this doesn't work
    assert(emptyTwitterDf().schema === actualTweetDf.schema)
    assert(!actualTweetDf.isEmpty)
    assert(actualTweetDf.count() === 6)
  }

  "getTweetDf" should "filter out not matching lines from the two datasets and return an empty union" in new Test {
    private val invalidLinesSampleDs = createStringDs(INVALID_LINES_SAMPLE_PATH)
    private val invalidLinesTwitterSamepleDs = createStringDs(INVALID_LINES_TWITTER_SAMPLE_PATH)
    when(hdfsDataServiceMock.retrieveSampleDataset(VALID_SAMPLE_PATH)).thenReturn(invalidLinesSampleDs)
    when(hdfsDataServiceMock.retrieveSampleDataset(VALID_TWITTER_SAMPLE_PATH)).thenReturn(invalidLinesTwitterSamepleDs)

    private val actualTweetDf = tweetDataService.getTweetDf()
//    assertDataFrameEquals(expectedDf, actualDf) //sad this doesn't work
    assert(emptyTwitterDf().schema === actualTweetDf.schema)
    assert(actualTweetDf.isEmpty)
  }

  "twitterSampleToTweetDf" should "return an empty dataframe " +
    "with created_at and text cols when the string dataset is empty" in new Test {

    private val emptyStringDs = getEmptyDataSet
    private val twitterSampleToTweetDf = PrivateMethod[DataFrame]('twitterSampleToTweetDf)

    private val actualDf = tweetDataService.invokePrivate(twitterSampleToTweetDf(emptyStringDs))
//    assertDataFrameEquals(expectedDf, actualDf) //sad this doesn't work
    assert(emptyTwitterDf().schema === actualDf.schema)
    assert(actualDf.isEmpty)
  }

  "twitterSampleToTweetDf" should "return dataframe " +
    "with created_at and text cols when a valid string dataset is passed" in new Test {

    private val validStringDs = createStringDs(VALID_TWITTER_SAMPLE_PATH)
    private val twitterSampleToTweetDf = PrivateMethod[DataFrame]('twitterSampleToTweetDf)

    private val actualDf = tweetDataService.invokePrivate(twitterSampleToTweetDf(validStringDs))
//    assertDataFrameEquals(expectedDf, actualDf) //sad this doesn't work
    assert(emptyTwitterDf().schema === actualDf.schema)
    assert(!actualDf.isEmpty)
    assert(actualDf.count() === 3)
  }

  "twitterSampleToTweetDf" should "filter out json lines " +
    "with empty or null created_at and text cols in the string dataset passed" in new Test {

    private val invalidStringDs = createStringDs(INVALID_TWITTER_SAMPLE_PATH)
    private val twitterSampleToTweetDf = PrivateMethod[DataFrame]('twitterSampleToTweetDf)

    private val actualDf = tweetDataService.invokePrivate(twitterSampleToTweetDf(invalidStringDs))
//    assertDataFrameEquals(expectedDf, actualDf) //sad this doesn't work
    assert(emptyTwitterDf().schema === actualDf.schema)
    assert(actualDf.isEmpty)
  }

  private def emptyTwitterDf(): DataFrame = {
    val sQLContext: SQLContext = spark.sqlContext
    import sQLContext.implicits._
    Seq.empty[(Timestamp, String)].toDF(Seq(CREATED_AT, TEXT): _*)
  }

  private def getEmptyDataSet: Dataset[String] = {
    val sQLContext: SQLContext = spark.sqlContext
    import sQLContext.implicits._

    spark.emptyDataset[String]
  }

  private def createStringDs(path: String): Dataset[String] = {
    spark.read.textFile(path)
  }

}
