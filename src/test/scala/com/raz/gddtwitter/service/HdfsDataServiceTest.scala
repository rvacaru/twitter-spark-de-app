package com.raz.gddtwitter.service

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{Dataset, SQLContext}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HdfsDataServiceTest extends FlatSpec with MockFactory with DatasetSuiteBase {

  private trait Test {
    val hdfsUtilServiceMock: HdfsUtilService = stub[HdfsUtilService]

    val hdfsDataService: HdfsDataService = new HdfsDataService(spark, hdfsUtilServiceMock)

    val mockDataset: Dataset[String] = createMockDataset()
  }

  "retrieveSampleDataset" should "return a non empty dataset when path file exists" in new Test {
    private val path: String = getClass.getResource("/testFile.txt").getPath()
    (hdfsUtilServiceMock.existsInHdfs _).when(path).returns(true)

    private val actualDataset: Dataset[String] = hdfsDataService.retrieveSampleDataset(path)

    (hdfsUtilServiceMock.existsInHdfs _).verify(path)
    assert(!actualDataset.isEmpty)
    assert(mockDataset.collect().toSet === actualDataset.collect().toSet)
  }

  "retrieveSampleDataset" should "return an empty dataset when path file doesn't exist" in new Test {
    private val invalidPath = "/path/not-existent.txt"
    (hdfsUtilServiceMock.existsInHdfs _).when(invalidPath).returns(false)

    private val actualDataset: Dataset[String] = hdfsDataService.retrieveSampleDataset(invalidPath)

    (hdfsUtilServiceMock.existsInHdfs _).verify(invalidPath)
    assert(actualDataset.isEmpty)
  }

  private def createMockDataset(): Dataset[String] = {
    val sQLContext: SQLContext = spark.sqlContext
    import sQLContext.implicits._

    sc.parallelize(Seq("one", "two", "three")).toDF().as[String]
  }

}
