package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ SQLContext, Row, DataFrame }
import com.typesafe.config.{ ConfigFactory, Config }
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import scala.io.Source
import java.io._

object Main {
  def main(args: Array[String]) {
    val onCluster = false

    // Spark environment
    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val pathSmall = "dblp_1k.csv"
    val (datasetSmall, rddSmall) = readResource(pathSmall, onCluster, sqlContext)

    //    val pathMedium = "dblp_3k.csv"
    //    val (datasetMedium, rddMedium) = readResource(pathMedium, onCluster, sqlContext)
    //
    //    val pathMedium2 = "dblp_5k.csv"
    //    val (datasetMedium2, rddMedium2) = readResource(pathMedium2, onCluster, sqlContext)
    //
    //    val pathBig = "dblp_10K.csv"
    //    val (datasetBig, rddBig) = readResource(pathBig, onCluster)

    testingInputSize(datasetSmall, rddSmall, "1K")
//    testingInputSize(datasetMedium, rddMedium, "3K")
//    testingInputSize(datasetMedium2, rddMedium2, "5K")
//    testingInputSize(datasetBig, rddBig, "10K")

    testingAnchorSize(datasetSmall, rddSmall, 4)
    testingAnchorSize(datasetSmall, rddSmall, 10)
    testingAnchorSize(datasetSmall, rddSmall, 20)
    testingAnchorSize(datasetSmall, rddSmall, 50)

    //    val inputFile = "../cs422-group13/test/dblp_1k.csv"
    //    val numAnchors = 4
    //    val distanceThreshold = 2
    //    val attrIndex = 0
    //    var output = ""
    //
    //    val path = "/Users/yawen/Documents/Scala/dblp_small.csv"
    ////    val inputFile = new File(path).getPath
    //
    //    //    val input = new File(getClass.getResource(inputFile).getFile).getPath
    //    val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[*]")
    //    val ctx = new SparkContext(sparkConf)
    //    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)
    //
    //    val df = sqlContext.read
    //      .format("com.databricks.spark.csv")
    //      .option("header", "true")
    //      .option("inferSchema", "true")
    //      .option("delimiter", ",")
    //      .load(inputFile)
    //
    //    val rdd = df.rdd
    //    val schema = df.schema.toList.map(x => x.name)
    //    val dataset = new Dataset(rdd, schema)
    //
    //    val t1 = System.nanoTime
    //    val sj = new SimilarityJoin(numAnchors, distanceThreshold)
    //    val res = sj.similarity_join(dataset, attrIndex)
    //
    //    val resultSize = res.count
    //    println("Cluster join count: " + resultSize)
    //    val t2 = System.nanoTime
    //
    //    val clusterResults = res.map( x=>
    //                                      if (x._1 < x._2)
    //                                        (x._1, x._2)
    //                                      else
    //                                        (x._2, x._1))
    //                             .sortBy(line => (line._1, line._2))
    //
    //    output += "Cluster join result: \n"
    //    for (x <- clusterResults.collect()) output += x.toString() + "\n"
    //
    //    output += "Cluster join: " + (t2 - t1) / (Math.pow(10, 9)) + "s" + "\n\n"
    //
    //    // cartesian
    //    val t1Cartesian = System.nanoTime
    //    val cartesian = rdd.map(x => (x(attrIndex), x)).cartesian(rdd.map(x => (x(attrIndex), x)))
    //      .filter(x => (x._1._2(attrIndex).toString() != x._2._2(attrIndex).toString() && sj.edit_distance(x._1._2(attrIndex).toString(), x._2._2(attrIndex).toString()) <= distanceThreshold))
    //
    //    println("Cartesian count: " + cartesian.count)
    //    val t2Cartesian = System.nanoTime
    //
    //
    //    val cartesianResults = cartesian.asInstanceOf[RDD[((String, Row), (String, Row))]]
    //                                    .map( x=> (x._1._1, x._2._1))
    //                                    .map( x=>
    //                                            if (x._1 < x._2)
    //                                              (x._1, x._2)
    //                                            else
    //                                              (x._2, x._1)
    //                                      )
    //                                      .sortBy(line => (line._1, line._2), ascending = true)
    //
    //
    //
    //
    //    output += "Cartesian join result: \n"
    //    for (y <- cartesianResults.collect()) output += y.toString() + "\n"
    //    output += "Cartesian join: " + (t2Cartesian - t1Cartesian) / (Math.pow(10, 9)) + "s \n"
    //
    //    print(output)
  }

  // Methods

  def readResource(file: String, onCluster: Boolean, sqlContext: SQLContext): (Dataset, RDD[Row]) = {
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
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(input)

    val rdd = df.rdd
    val schema = df.schema.toList.map(x => x.name)
    val dataset = new Dataset(rdd, schema)

    (dataset, rdd)
  }

  def testingInputSize(dataset: Dataset, rdd: RDD[Row], size: String) = {
    val numAnchors = 30
    val distanceThreshold = 2
    val attrIndex = 0
    var outputInputSize = ""

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

    outputInputSize += "Testing input " + size + "\n"
    outputInputSize += "Cluster join count: " + resultSize + "\n"
    outputInputSize += "Cluster join time: " + (t2 - t1) / (Math.pow(10, 9)) + "s\n"
    outputInputSize += "Cartesian count: " + cartesianCount + "\n"
    outputInputSize += "Cartesian join time: " + (t2Cartesian - t1Cartesian) / (Math.pow(10, 9)) + "s\n"

    checkCorrectness(res, cartesian, outputInputSize)
  }

  def testingAnchorSize(dataset: Dataset, rdd: RDD[Row], numAnchors: Int) = {
    val distanceThreshold = 2
    val attrIndex = 0
    var outputAnchorSize = ""

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

    outputAnchorSize += "Testing with " + numAnchors + " anchors " + "\n"
    outputAnchorSize += "Cluster join count: " + resultSize + "\n"
    outputAnchorSize += "Cluster join time: " + (t2 - t1) / (Math.pow(10, 9)) + "s" + "\n"
    outputAnchorSize += "Cartesian count: " + cartesianCount + "\n"
    outputAnchorSize += "Cartesian join time: " + (t2Cartesian - t1Cartesian) / (Math.pow(10, 9)) + "s" + "\n"

    checkCorrectness(res, cartesian, outputAnchorSize)

  }

  def checkCorrectness(cluster: RDD[(String, String)], cartesian: RDD[((Any, Row), (Any, Row))], output: String) = {
    var outputCorrectness = output

    // Correctness
    // Sort and check result
    val clusterResults = cluster.map(x =>
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
          (x._2, x._1))
      .sortBy(line => (line._1, line._2), ascending = true)

    val diff = cartesianResults
      .zip(clusterResults)
      .collect {
        case (a, b) if a != b =>
          outputCorrectness += "Difference : " + a + " vs " + b + "\n"
          a -> b
      }

    val diffCount = diff.count()
    outputCorrectness += "Difference count: " + diffCount + "\n"
    println(outputCorrectness)
  }
}
