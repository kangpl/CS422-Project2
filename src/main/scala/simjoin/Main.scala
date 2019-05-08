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
    val inputFile = "../cs422-group13/test/dblp_1k.csv"
    val numAnchors = 4
    val distanceThreshold = 2
    val attrIndex = 0

    //val path = "/Users/yawen/Documents/Scala/dblp_small.csv"
    //val input = new File(path).getPath

    //    val input = new File(getClass.getResource(inputFile).getFile).getPath
    val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(inputFile)

    val rdd = df.rdd
    val schema = df.schema.toList.map(x => x.name)
    val dataset = new Dataset(rdd, schema)

    val t1 = System.nanoTime
    val sj = new SimilarityJoin(numAnchors, distanceThreshold)
    val res = sj.similarity_join(dataset, attrIndex)

    val resultSize = res.count
    println("Cluster join count: " + resultSize)
    val t2 = System.nanoTime
    
    val clusterResults = res.map( x=> 
                                      if (x._1 < x._2)
                                        (x._1, x._2)
                                      else
                                        (x._2, x._1))
                             .sortBy(line => (line._1, line._2))
    clusterResults.foreach(x => println(x))
    println()
    println("Cluster join: " + (t2 - t1) / (Math.pow(10, 9)))

    // cartesian
    val t1Cartesian = System.nanoTime
    val cartesian = rdd.map(x => (x(attrIndex), x)).cartesian(rdd.map(x => (x(attrIndex), x)))
      .filter(x => (x._1._2(attrIndex).toString() != x._2._2(attrIndex).toString() && sj.edit_distance(x._1._2(attrIndex).toString(), x._2._2(attrIndex).toString()) <= distanceThreshold))

    println("Cartesian count: " + cartesian.count)
    val t2Cartesian = System.nanoTime
    
    
    val cartesianResults = cartesian.asInstanceOf[RDD[((String, Row), (String, Row))]]
                                    .map( x=> (x._1._1, x._2._1))
                                    .map( x=> 
                                            if (x._1 < x._2)
                                              (x._1, x._2)
                                            else
                                              (x._2, x._1)
                                      ).distinct
                                      .sortBy(line => (line._1, line._2), ascending = true)
                                                
                                        
                                          
//    cartesian.foreach(x => println(x))
//    println()
        
    cartesianResults.foreach(x => println(x))
    println()
    println("Cartesian join: " + (t2Cartesian - t1Cartesian) / (Math.pow(10, 9)))
  }
}
