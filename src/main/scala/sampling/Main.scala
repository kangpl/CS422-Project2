package sampling

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql._
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
    val sqlContext:SQLContext = new org.apache.spark.sql.SQLContext(ctx)


//    val path = "/Users/yawen/Documents/Scala/tpch_parquet_sf1/lineitem.parquet"
    //    val path = "../CS422-Project2/src/main/resources/tpch_parquet_sf1/lineitem.parquet"
//    val lineitem = sqlContext.read.load(path)

    
    val inputFile = "../lineitem.tbl"
    val input = new File(getClass.getResource(inputFile).getFile).getPath
    
    val customSchema = StructType(Array(
	    StructField("l_orderkey", LongType, true),
	    StructField("l_partkey", LongType, true),
	    StructField("l_suppkey", LongType, true),
	    StructField("l_linenumber", IntegerType, true),
	    StructField("l_quantity", DoubleType, true),
	    StructField("l_extendedprice", DoubleType, true),
	    StructField("l_discount", DoubleType, true),
	    StructField("l_tax", DoubleType, true),
	    StructField("l_returnflag", StringType, true),
	    StructField("l_linestatus", StringType, true),
	    StructField("l_shipdate", DateType, true),
	    StructField("l_commitdate", DateType, true),
	    StructField("l_receiptdate", DateType, true),
	    StructField("l_shipinstruct", StringType, true),
	    StructField("l_shipmode", StringType, true),
	    StructField("l_comment", StringType, true)))
	    
    val lineitem = sqlContext.read
	      .format("com.databricks.spark.csv")
	      .option("header", "false")
	      .option("delimiter", "|")
	      .schema(customSchema)
	      .load(input)
	      
//    val lineitem = sqlContext.read.load("../CS422-Project2/src/main/resources/tpch_parquet_sf1/lineitem.parquet")
      
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
    desc_.e = 0.01
    desc_.ci = 0.95
    lineitem.show(5)
    lineitem.printSchema()

    val tmp = Sampler.sample(desc_.lineitem, 1000000, desc_.e, desc_.ci)
    //    desc.samples = tmp._1
    //    desc.sampleDescription = tmp._2
    //
    //    // check storage usage for samples
    //
    //    // Execute first query
    //    Executor.execute_Q1(desc, session, List("3"))
  }
  
}
