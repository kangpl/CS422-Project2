package cubeoperator

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object Main {

  def main(args: Array[String]) {
    val onCluster = true

    val reducers = 4
    val cb = new CubeOperator(reducers)

    val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val pathSmall = "lineorder_small.tbl"
    val pathMedium = "lineorder_medium.tbl"
    val pathBig = "lineorder_big.tbl"

    val (datasetSmall, dfSmall) = readResource(pathSmall, onCluster, sqlContext)
    val (datasetMedium, dfMedium) = readResource(pathMedium, onCluster, sqlContext)
    val (datasetBig, dfBig) = readResource(pathBig, onCluster, sqlContext)

    inputSizeTest(cb, datasetSmall, dfSmall, "Small")
    inputSizeTest(cb, datasetMedium, dfMedium, "Medium")
    inputSizeTest(cb, datasetBig, dfBig, "Big")

    val groupingList3 = List("lo_suppkey", "lo_shipmode", "lo_orderdate")
    cubeAttributeTest(cb, datasetMedium, groupingList3, "3")

    val groupingList4 = List("lo_suppkey", "lo_shipmode", "lo_orderdate", "lo_orderkey")
    cubeAttributeTest(cb, datasetMedium, groupingList4, "4")

    val groupingList5 = List("lo_suppkey", "lo_shipmode", "lo_orderdate", "lo_orderkey", "lo_orderpriority")
    cubeAttributeTest(cb, datasetMedium, groupingList5, "5")

    val groupingList6 = List("lo_suppkey", "lo_shipmode", "lo_orderdate", "lo_orderkey", "lo_orderpriority", "lo_shippriority")
    cubeAttributeTest(cb, datasetMedium, groupingList6, "6")

    reducerSizeTest(cb, datasetMedium, "4")
    reducerSizeTest(new CubeOperator(8), datasetMedium, "8")
    reducerSizeTest(new CubeOperator(12), datasetMedium, "12")
    reducerSizeTest(new CubeOperator(16), datasetMedium, "16")
    reducerSizeTest(new CubeOperator(20), datasetMedium, "20")

  }

  def parse(x: Row, aggType: String): (String, Double) = {
    var res: String = ""
    if (x(0) != null && (x(1) != null || x(2) != null)) res += x(0) + "_"
    else if (x(0) != null) res += x(0)

    if (x(1) != null && x(2) != null) res += x(1) + "_" + x(2)
    else if (x(1) != null) res += x(1)
    else if (x(2) != null) res += x(2)

    val result = aggType match {
      case "AVG"   => (res, x.getDouble(3))
      case "COUNT" => (res, x.getLong(3).toDouble)
      case "SUM"   => (res, x.getLong(3).toDouble)
      case _       => (res, x.getInt(3).toDouble)

    }
    result
  }

  // Methods

  def timer[R](block: => (String, R)): R = {
    val t0 = System.nanoTime()
    val result = block._2 // call-by-name
    val t1 = System.nanoTime()
    println(block._1 + "'s execution time: " + (t1 - t0) / (1e9) + "s")
    result
  }

  def readResource(file: String, onCluster: Boolean, sqlContext: SQLContext): (Dataset, DataFrame) = {
    var input = ""
    if (onCluster) {
      input = "../cs422-group13/test/" + file
    } else {
      val path = "/Users/yawen/Documents/Scala/"
      //      val path = "../"
      input = new File(path + file).getPath
      // input = new File(getClass.getResource(path).getFile).getPath
    }

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