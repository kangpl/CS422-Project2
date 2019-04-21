package cubeoperator

import org.scalatest._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._


class Parser extends java.io.Serializable {
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
}

class Task1Test extends FlatSpec {

  // Set up Environment
  val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
  val ctx = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(ctx)
  
  val parser = new Parser()
  
  "Cube naive and MRCube" should "give same results for small dataset" in {
    val reducers = 4
    val cb = new CubeOperator(reducers)

    // Read in the datasets
    val pathSmall = "/Users/yawen/Documents/Scala/lineorder_small.tbl"
    //  val pathSmall = "../lineorder_small.tbl"
    val (datasetSmall, dfSmall) = readResource(pathSmall)

    inputSizeTest(cb, datasetSmall, dfSmall, "Small")
  }

  //  it should "give same results for medium dataset" in {
  //    val reducers = 4
  //    val cb = new CubeOperator(reducers)
  //
  //    val pathMedium = "/Users/yawen/Documents/Scala/lineorder_medium.tbl"
  //    //  val pathMedium = "../lineorder_medium.tbl"
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
  //    //  val pathBig = "../lineorder_big.tbl"
  //    val (datasetBig, dfBig) = readResource(pathBig)
  //
  //    inputSizeTest(cb, datasetBig, dfBig, "Big")
  //
  //  }

  // Methods
  //  def currentMethodName(): String = Thread.currentThread.getStackTrace()(2).getMethodName

  def timer[R](block: => (String, R)): R = {
    val t0 = System.nanoTime()
    val result = block._2 // call-by-name
    val t1 = System.nanoTime()
    println(block._1 + "'s execution time: " + (t1 - t0) + "ns")
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
    val res_MRCube = timer { ("MRCube_" + size, cb.cube(dataset, groupingList, "lo_supplycost", op)) }
    val res_cubeNaive = timer { ("cubeNaive_" + size, cb.cube_naive(dataset, groupingList, "lo_supplycost", op)) }

    // Compare results
    //    val resultSorted_MRCube = res_MRCube.sortBy(_._1, ascending = false)
    //    val resultSorted_cubeNaive = res_cubeNaive.sortBy(_._1, ascending = false)

    val sql = df.cube("lo_suppkey", "lo_shipmode", "lo_orderdate")
      .agg(count("lo_supplycost") as "sum supplycost")

    val sqlRdd: RDD[Row] = sql.rdd
    val sqlMapped = sqlRdd.map(row => parser.parse(row, op))
    val result_sql = sqlMapped.sortBy(_._1, ascending = false)

    //    val diff_MRCube = resultSorted_MRCube.join(result_sql).collect {
    //      case (k, (s1, s2)) if s1 == s2 => (k, s1, s2)
    //    }
    //
    //    val diff_cubeNaive = resultSorted_cubeNaive.join(result_sql).collect {
    //      case (k, (s1, s2)) if s1 == s2 => (k, s1, s2)
    //    }
    //
    //    assert(diff_MRCube.isEmpty)
    //    assert(diff_cubeNaive.isEmpty)
    //    assert(diff_MRCube.count() == diff_cubeNaive.count())

  }

}