package com.raz.gddtwitter.service

import com.holdenkarau.spark.testing.{Column, DataframeGenerator, DatasetSuiteBase, SharedSparkContext}
import com.raz.gddtwitter.service.TestSchemaConstants._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.runner.RunWith
import org.mockito.Mockito.{mock, when, withSettings}
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalatest.check.Checkers
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, PrivateMethodTester}

@RunWith(classOf[JUnitRunner])
class TrendTopicsServiceTest extends FlatSpec with PrivateMethodTester
  with DatasetSuiteBase with SharedSparkContext with Checkers {

  private trait Test {
    val tweetDataServiceMock: TweetDataService = mock(classOf[TweetDataService], withSettings().serializable())

    val trendTopicsService: TrendTopicsService = new TrendTopicsService(spark, tweetDataServiceMock)

    val sqlContext: SQLContext = new SQLContext(sc)
    val tweetDfSchema: StructType = getTweetDfSchema()
    val topicsDfSchema: StructType = getTopicsDfSchema()
    val getTopicsDfMethod: PrivateMethod[DataFrame] = PrivateMethod[DataFrame]('getTopicsDf)
    val groupTopicsPerWindowMethod: PrivateMethod[DataFrame] = PrivateMethod[DataFrame]('groupTopicsPerWindow)

  }

//  "getTopicsDf" should "returns a topics DF with certain schema and size >= tweetDf size, from the tweet lines DF" in new Test {
//    private val dataframeGen = DataframeGenerator.arbitraryDataFrame(sqlContext, tweetDfSchema)
//    private val property = forAll(dataframeGen.arbitrary) {
//      tweetDf => {
//        when(tweetDataServiceMock.getTweetDf()).thenReturn(tweetDf)
//        val actualTopicsDf = trendTopicsService.invokePrivate(getTopicsDfMethod())
//
//        tweetDf.schema === tweetDfSchema &&
//          actualTopicsDf.schema === topicsDfSchema &&
//          actualTopicsDf.count() >= 0
//      }
//    }
//    check(property)
//  }
//
//  "getTopicsDf" should "returns a topics DF with double the size of a tweetDf containing lines with 2 words" +
//    "and the topics Df contains exclusively lowercase and trimmed strings" in new Test {
//    val textColGenerator = new Column(TEXT, Gen.oneOf(" Only Two ", " Not More "))
//    private val dataframeGen = DataframeGenerator.arbitraryDataFrameWithCustomFields(sqlContext, tweetDfSchema)(textColGenerator)
//
//    private val property = forAll(dataframeGen.arbitrary) {
//      tweetDf => {
//        when(tweetDataServiceMock.getTweetDf()).thenReturn(tweetDf)
//        val actualTopicsDf = trendTopicsService.invokePrivate(getTopicsDfMethod())
//        val actualSetOfTopics: Set[String] = actualTopicsDf.select(TOPIC).distinct().collect().to[Set].map(row => row.getString(0))
//
//          actualTopicsDf.count() === tweetDf.count() * 2 &&
//          (
//            actualSetOfTopics.intersect(Set[String]("only", "two", "not", "more")).nonEmpty ||
//            actualSetOfTopics.isEmpty
//          )
//      }
//    }
//    check(property)
//  }

// Test not working
  "getTopicsDf" should "filter out punctuation from tweetDf" ignore new Test {
    val textColGenerator = new Column(TEXT, Gen.oneOf(",.a?b!c*-+"))
    private val dataframeGen = DataframeGenerator.arbitraryDataFrameWithCustomFields(sqlContext, tweetDfSchema)(textColGenerator)

    private val property = forAll(dataframeGen.arbitrary) {
      tweetDf => {
        when(tweetDataServiceMock.getTweetDf()).thenReturn(tweetDf)
        val actualTopicsDf = trendTopicsService.invokePrivate(getTopicsDfMethod())
        val actualSetOfTopics: Set[String] = actualTopicsDf.select(TOPIC).distinct().collect().to[Set].map(row => row.getString(0))

        actualSetOfTopics === Set("abc") || actualSetOfTopics.isEmpty
      }
    }
    check(property)
  }

  "groupTopicsPerWindow" should "create windows of correct size related to list of topics" in new Test {
    val topicColGenerator = new Column(TOPIC, Gen.oneOf("ik", "ben", "raz"))
    private val dataframeGen = DataframeGenerator.arbitraryDataFrameWithCustomFields(sqlContext, topicsDfSchema)(topicColGenerator)

    private val property = forAll(dataframeGen.arbitrary) {
      topicsDf => {
        val actualTopicsWindowsDf = trendTopicsService.invokePrivate(groupTopicsPerWindowMethod(topicsDf, "1 day"))
//        val actualSetOfTopics: Set[String] = actualTopicsDf.select(TOPIC).distinct().collect().to[Set].map(row => row.getString(0))

        val array = actualTopicsWindowsDf.withColumn("dateDiff", datediff(col(WINDOW_END), col(WINDOW_START)))
          .select(col("dateDiff")).collect().map(row => row.getTimestamp(0))

        actualTopicsWindowsDf.schema === getTopicsWindowsDfSchema()
        actualTopicsWindowsDf.count() < topicsDf.count()


      }
    }
    check(property)
  }

  private def getTweetDfSchema(): StructType = {
    StructType(List(StructField(CREATED_AT, TimestampType), StructField(TEXT, StringType)))
  }

  private def getTopicsDfSchema(): StructType = {
    StructType(List(StructField(CREATED_AT, TimestampType), StructField(TOPIC, StringType)))
  }

//  private def getTopicsWindowsDfSchema(): StructType = {
//    new StructType()
//    .add(WINDOW, new StructType()
//      .add(START, TimestampType)
//      .add(END, TimestampType))
//    .add(TOPICS, ArrayType(StringType))
//  }

  private def getTopicsWindowsDfSchema(): StructType = {
    StructType(List(
      StructField(WINDOW, StructType(List(StructField(START, TimestampType), StructField(END, TimestampType)))),
      StructField(TOPICS, ArrayType(StringType)))
    )
  }

}
