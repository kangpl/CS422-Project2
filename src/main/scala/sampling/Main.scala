package sampling

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Row, SparkSession }
import java.io._


object Main {
  def main(args: Array[String]) {

    //    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    //    val sc = SparkContext.getOrCreate()
    //    val session = SparkSession.builder().getOrCreate();
    //
    //    val rdd = RandomRDDs.uniformRDD(sc, 100000)
    //    val rdd2 = rdd.map(f => Row.fromSeq(Seq(f * 2, (f*10).toInt)))
    //
    //    val table = session.createDataFrame(rdd2, StructType(
    //      StructField("A1", DoubleType, false) ::
    //      StructField("A2", IntegerType, false) ::
    //      Nil
    //    ))

//    val path = "/Users/yawen/Documents/Scala/lineitem.tbl"
//    val input = new File(path).getPath
    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)
    
    val lineitem = sqlContext.read.load("../CS422-Project2/src/main/resources/tpch_parquet_sf1/lineitem.parquet")
      
//    val customer = sqlContext.read.load("/cs422-data/tpch/sf100/parquet/customer.parquet")
//    val nation = sqlContext.read.load("/cs422-data/tpch/sf100/parquet/nation.parquet")
//    val lineitem = sqlContext.read.load("/cs422-data/tpch/sf100/parquet/lineitem.parquet")
//    val order = sqlContext.read.load("/cs422-data/tpch/sf100/parquet/order.parquet")
//    val part = sqlContext.read.load("/cs422-data/tpch/sf100/parquet/part.parquet")
//    val partsupp = sqlContext.read.load("/cs422-data/tpch/sf100/parquet/partsupp.parquet")
//    val region = sqlContext.read.load("/cs422-data/tpch/sf100/parquet/region.parquet")
//    val supplier = sqlContext.read.load("/cs422-data/tpch/sf100/parquet/supplier.parquet")
      
    var desc_ = new Description
    desc_.lineitem = lineitem
    desc_.e = 0.1
    desc_.ci = 0.95
    lineitem.show(5)
    lineitem.printSchema()
    


    val tmp = Sampler.sample(desc_.lineitem, 1000000, desc_.e, desc_.ci, ctx)
    //    desc.samples = tmp._1
    //    desc.sampleDescription = tmp._2
    //
    //    // check storage usage for samples
    //
    //    // Execute first query
    //    Executor.execute_Q1(desc, session, List("3"))
  }
}
