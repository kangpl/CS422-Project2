package cubeoperator

import org.scalatest._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

class Task1Test extends FlatSpec {

  // Set up Environment
  val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
  val ctx = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

  // Methods
  def currentMethodName(): String = Thread.currentThread.getStackTrace()(2).getMethodName

  def timer[R](block: => (String, R)): R = {
    val t0 = System.nanoTime()
    val result = block._2 // call-by-name
    val t1 = System.nanoTime()
    println(block._1 + "'s execution time: " + (t1 - t0) + "ns")
    result
  }

  def readResource(path: String): Dataset = {
    val input = new File(path).getPath
    //    val input = new File(getClass.getResource(inputFile).getFile).getPath

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(input)

    val rdd = df.rdd

    val schema = df.schema.toList.map(x => x.name)

    val dataset = new Dataset(rdd, schema)

    dataset
  }

  // Tests
  def varyingInputSize() {
    val reducers = 4
    val cb = new CubeOperator(reducers)

    // Read in the datasets
    val pathSmall = "/Users/yawen/Documents/Scala/lineorder_small.tbl"
    //  val pathSmall = "../lineorder_small.tbl"
    val datasetSmall = readResource(pathSmall)

    val pathMedium = "/Users/yawen/Documents/Scala/lineorder_medium.tbl"
    //  val pathMedium = "../lineorder_medium.tbl"
    val datasetMedium = readResource(pathMedium)

    val pathBig = "/Users/yawen/Documents/Scala/lineorder_big.tbl"
    //  val pathBig = "../lineorder_big.tbl"
    val datasetBig = readResource(pathBig)

    // Start testing
    inputSizeTest(cb, datasetSmall, "Small")
    inputSizeTest(cb, datasetMedium, "Medium")
    inputSizeTest(cb, datasetBig, "Big")

  }

  def inputSizeTest(cb: CubeOperator, dataset: Dataset, size: String) {
    var groupingList = List("lo_suppkey", "lo_shipmode", "lo_orderdate")

    val res_MRCube = timer { ("MRCube_" + size, cb.cube(dataset, groupingList, "lo_supplycost", "SUM")) }
    val res_cubeNaive = timer { ("cubeNaive_" + size, cb.cube_naive(dataset, groupingList, "lo_supplycost", "SUM")) }

    assert(1 == 1)
  }

  varyingInputSize

}