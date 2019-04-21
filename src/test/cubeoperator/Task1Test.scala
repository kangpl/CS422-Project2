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

  //  "Cube naive and MRCube" should "give same results for small dataset" in {
  //    val reducers = 4
  //    val cb = new CubeOperator(reducers)
  //
  //    // Read in the datasets
  //    val pathSmall = "/Users/yawen/Documents/Scala/lineorder_small.tbl"
  //    //    val pathSmall = "../lineorder_small.tbl"

  //    val (datasetSmall, dfSmall) = readResource(pathSmall)
  //
  //    inputSizeTest(cb, datasetSmall, dfSmall, "Small")
  //  }
  //
  //  it should "give same results for medium dataset" in {
  //    val reducers = 4
  //    val cb = new CubeOperator(reducers)
  //
  //    val pathMedium = "/Users/yawen/Documents/Scala/lineorder_medium.tbl"
  //    //    val pathMedium = "../lineorder_medium.tbl"
  //    val (datasetMedium, dfMedium) = readResource(pathMedium)
  //
  //    inputSizeTest(cb, datasetMedium, dfMedium, "Medium")
  //
  //  }
  //
  //  it should "give same results for big dataset" in {
  //    val reducers = 4
  //    val cb = new CubeOperator(reducers)
  //
  //    val pathBig = "/Users/yawen/Documents/Scala/lineorder_big.tbl"
  //    //    val pathBig = "../lineorder_big.tbl"
  //    val (datasetBig, dfBig) = readResource(pathBig)
  //
  //    inputSizeTest(cb, datasetBig, dfBig, "Big")
  //
  //  }

  val pathMedium = "/Users/yawen/Documents/Scala/lineorder_medium.tbl"
  //    val pathMedium = "../lineorder_medium.tbl"
  val (datasetMedium, dfMedium) = readResource(pathMedium)

  "Cube naive and MRCube attribute test" should "give same results for 3 attributes" in {
    val reducers = 4
    val cb = new CubeOperator(reducers)
    val groupingList = List("lo_suppkey", "lo_shipmode", "lo_orderdate")

    cubeAttributeTest(cb, datasetMedium, groupingList, "3")
  }

  it should "give same results for 4 attributes" in {
    val reducers = 4
    val cb = new CubeOperator(reducers)
    val groupingList = List("lo_suppkey", "lo_shipmode", "lo_orderdate", "lo_orderkey")

    cubeAttributeTest(cb, datasetMedium, groupingList, "4")
  }

  it should "give same results for 5 attributes" in {
    val reducers = 4
    val cb = new CubeOperator(reducers)
    val groupingList = List("lo_suppkey", "lo_shipmode", "lo_orderdate", "lo_orderkey", "lo_orderpriority")

    cubeAttributeTest(cb, datasetMedium, groupingList, "5")
  }

  it should "give same results for 6 attributes" in {
    val reducers = 4
    val cb = new CubeOperator(reducers)
    val groupingList = List("lo_suppkey", "lo_shipmode", "lo_orderdate", "lo_orderkey", "lo_orderpriority", "lo_shippriority")

    cubeAttributeTest(cb, datasetMedium, groupingList, "6")
  }

  "Cube naive and MRCube reducer test" should "give same results for 4 reducers" in {
    val reducers = 4
    val cb = new CubeOperator(reducers)

    reducerSizeTest(cb, datasetMedium, "4")

  }

  it should "give same results for 8 reducers" in {
    val reducers = 8
    val cb = new CubeOperator(reducers)

    reducerSizeTest(cb, datasetMedium, "8")

  }

  it should "give same results for medium dataset 12 reducers" in {
    val reducers = 12
    val cb = new CubeOperator(reducers)

    reducerSizeTest(cb, datasetMedium, "12")

  }

  it should "give same results for medium dataset 16 reducers" in {
    val reducers = 16
    val cb = new CubeOperator(reducers)

    reducerSizeTest(cb, datasetMedium, "16")

  }

  it should "give same results for medium dataset 20 reducers" in {
    val reducers = 20
    val cb = new CubeOperator(reducers)

    reducerSizeTest(cb, datasetMedium, "20")
  }

  // Methods
  //  def currentMethodName(): String = Thread.currentThread.getStackTrace()(2).getMethodName

  def timer[R](block: => (String, R)): R = {
    val t0 = System.nanoTime()
    val result = block._2 // call-by-name
    val t1 = System.nanoTime()
    println(block._1 + "'s execution time: " + (t1 - t0) / (1e9) + "s")
    result
  }

  def readResource(path: String): (Dataset, DataFrame) = {
    val input = new File(path).getPath
    //    val input = new File(getClass.getResource(path).getFile).getPath

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(input)

    val rdd = df.rdd

    val schema = df.schema.toList.map(x => x.name)

    val dataset = new Dataset(rdd, schema)

    (dataset, df)
  }

  // Tests
  def inputSizeTest(cb: CubeOperator, dataset: Dataset, df: DataFrame, size: String) {
    var groupingList = List("lo_suppkey", "lo_shipmode", "lo_orderdate")
    val op = "SUM"

    // Evaluate and count execution time
    val res_MRCube = timer { ("MRCube_input_" + size, cb.cube(dataset, groupingList, "lo_supplycost", op).count()) }
    val res_cubeNaive = timer { ("cubeNaive_input_" + size, cb.cube_naive(dataset, groupingList, "lo_supplycost", op).count()) }

    // Compare results

    assert(res_MRCube == res_cubeNaive)

  }

  def cubeAttributeTest(cb: CubeOperator, dataset: Dataset, groupingList: List[String], size: String) {
    val op = "SUM"
    // Evaluate and count execution time
    val res_MRCube = timer { ("MRCube_attribute_" + size, cb.cube(dataset, groupingList, "lo_supplycost", op).count()) }
    val res_cubeNaive = timer { ("cubeNaive_attribute_" + size, cb.cube_naive(dataset, groupingList, "lo_supplycost", op).count()) }

    // Compare results

    assert(res_MRCube == res_cubeNaive)

  }

  def reducerSizeTest(cb: CubeOperator, dataset: Dataset, size: String) {
    val op = "SUM"
    var groupingList = List("lo_suppkey", "lo_shipmode", "lo_orderdate")

    // Evaluate and count execution time
    val res_MRCube = timer { ("MRCube_reducer_" + size, cb.cube(dataset, groupingList, "lo_supplycost", op).count()) }
    val res_cubeNaive = timer { ("cubeNaive_reducer_" + size, cb.cube_naive(dataset, groupingList, "lo_supplycost", op).count()) }

    // Compare results

    assert(res_MRCube == res_cubeNaive)
  }

}