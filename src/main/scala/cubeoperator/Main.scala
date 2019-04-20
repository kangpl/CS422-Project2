package cubeoperator

import org.scalatest._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object Main {
  def main(args: Array[String]) {
    val reducers = 10

//    val inputFile = "../lineorder_small.tbl"
//    val path = "/Users/yawen/Documents/Scala/lineorder_small.tbl"
//    val input = new File(getClass.getResource(inputFile).getFile).getPath
//    val input = new File(path).getPath
    
    val inputFile= "../lineorder_small2.tbl"
    val input = new File(getClass.getResource(inputFile).getFile).getPath

    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(input)

    val rdd = df.rdd
        
    val schema = df.schema.toList.map(x => x.name)

    val dataset = new Dataset(rdd, schema)

    val cb = new CubeOperator(reducers)

    var groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate")
    val res = cb.cube_naive(dataset, groupingList, "lo_supplycost", "SUM")

    var result = res
        .sortBy(_._1, ascending = false)
//    result.collect().foreach(x => println(x))  
    
    println("=============================")
//    val res = cb.cube_naive(dataset, groupingList, "lo_supplycost", "SUM")
    /*
       The above call corresponds to the query:
       SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
       FROM LINEORDER
       CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
     */
    def parse(x: Row): (String, Double) = {
      var res:String = ""
      if (x(0)!=null && (x(1) != null || x(2) != null)) res += x(0) + "_"
      else if (x(0) != null) res += x(0)
      
      
      if (x(1) != null && x(2) != null) res += x(1) + "_" + x(2)
      else if (x(1) != null) res += x(1)
      else if (x(2) != null) res += x(2)
        
      (res, x.getLong(3).toDouble)
    }

    //Perform the same query using SparkSQL
    val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
      .agg(sum("lo_supplycost") as "sum supplycost")
    val q1Rdd: RDD[Row] = q1.rdd
    val q1Mapped = q1Rdd.map(row => (parse(row)))
    
    var result2 = q1Mapped.sortBy(_._1, ascending = false)
//    result2.collect().foreach(x => println(x))  

//    sqlres.sort(desc($"col1".desc)
    val indexValues = (0 until result.count().toInt).toList
    
    val diff = result.join(result2).collect {
        case (k, (s1, s2)) if s1 != s2 => (k, s1)
}
    diff.collect().foreach(x => println(x)) 
    println(diff)
    
//    for {
//      i <- (0 until result.count().toInt)
//    } yield(assert(result.values == result2(i)._2)
    
  }
  

  
}