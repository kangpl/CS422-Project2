package simjoin

import org.scalatest._

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ SQLContext, Row, DataFrame }
import com.typesafe.config.{ ConfigFactory, Config }
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import scala.io.Source
import java.io._

class Task2Test extends FlatSpec {

  // Spark environment
  val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[*]")
  val ctx = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

  val numAnchors = 4
  val distanceThreshold = 2
  val attrIndex = 0

  "Clusterjoin and Catesian join" should "give same results for 1k dataset" in {
    // Read in the datasets
    val pathSmall = "/Users/yawen/Documents/Scala/dblp_1k.csv"
    //    val pathSmall = "../dblp_1k.csv"

    val (datasetSmall, rddSmall) = readResource(pathSmall)

    testingInputSize(datasetSmall, rddSmall, "1K")
  }
  
  it should "give same results for 3K dataset" in {
    val pathMedium = "/Users/yawen/Documents/Scala/dblp_3K.csv"
    //    val pathMedium = "../dblp_5K.csv"
    val (datasetMedium, rddMedium) = readResource(pathMedium)

    testingInputSize(datasetMedium, rddMedium, "3K")

  }

  it should "give same results for 5K dataset" in {
    val pathMedium = "/Users/yawen/Documents/Scala/dblp_5K.csv"
    //    val pathMedium = "../dblp_5K.csv"
    val (datasetMedium, rddMedium) = readResource(pathMedium)

    testingInputSize(datasetMedium, rddMedium, "5K")

  }

  it should "give same results for 10K dataset" in {
    val pathBig = "/Users/yawen/Documents/Scala/dblp_10K.csv"
    //    val pathBig = "../dblp_10K.csv"
    val (datasetBig, rddBig) = readResource(pathBig)

    testingInputSize(datasetBig, rddBig, "10K")

  }

  // Methods

  def readResource(path: String): (Dataset, RDD[Row]) = {
    val input = new File(path).getPath
    //    val input = new File(getClass.getResource(path).getFile).getPath

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(input)

    val rdd = df.rdd
    val schema = df.schema.toList.map(x => x.name)
    val dataset = new Dataset(rdd, schema)

    (dataset, rdd)
  }

  def testingInputSize(dataset:Dataset, rdd:RDD[Row], size:String) = {
    val attrIndex = 0
    val numAnchors = 4
    val distanceThreshold = 2

    // Cluster join
    val t1 = System.nanoTime
    val sj = new SimilarityJoin(numAnchors, distanceThreshold)
    val res = sj.similarity_join(dataset, attrIndex)
    val resultSize = res.count
    val t2 = System.nanoTime

    // cartesian
    val t1Cartesian = System.nanoTime
    val cartesian = rdd.map(x => (x(attrIndex), x)).cartesian(rdd.map(x => (x(attrIndex), x)))
      .filter(x => (x._1._2(attrIndex).toString() != x._2._2(attrIndex).toString() && sj.edit_distance(x._1._2(attrIndex).toString(), x._2._2(attrIndex).toString()) <= distanceThreshold))
    val cartesianCount = cartesian.count
    val t2Cartesian = System.nanoTime

    println("Cluster join count: " + resultSize)
    println("Cluster join time: " + (t2 - t1) / (Math.pow(10, 9)) + "s")
    println("Cartesian count: " + cartesianCount)
    println("Cartesian join time: " + (t2Cartesian - t1Cartesian) / (Math.pow(10, 9)) + "s")

    // Correctness
    // Sort and check result
    val clusterResults = res.map(x =>
      if (x._1 < x._2)
        (x._1, x._2)
      else
        (x._2, x._1))
      .sortBy(line => (line._1, line._2))

    val cartesianResults = cartesian.asInstanceOf[RDD[((String, Row), (String, Row))]]
      .map(x => (x._1._1, x._2._1))
      .map(x =>
        if (x._1 < x._2)
          (x._1, x._2)
        else
          (x._2, x._1)).distinct
      .sortBy(line => (line._1, line._2), ascending = true)
      
    val diff = cartesianResults.join(clusterResults).collect {
      case (k, (s1, s2)) if s1 != s2 =>
        println("Difference: " + k + ": " + s1 + " vs " + s2)
        (k, s1, s2)
    }

    val diffCount = diff.count()
    assert(diffCount == 0)

  }

}