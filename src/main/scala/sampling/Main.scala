package sampling

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.io._

object Main {
  def main(args: Array[String]) {

    val onCluster = true
    var sparkConf: SparkConf = null
    var path = ""

    if (onCluster) {
      sparkConf = new SparkConf().setAppName("CS422-Project2")
      path = "/cs422-data/tpch/sf10/parquet/"
    } else {
      sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
      path = "/Users/yawen/Documents/Scala/tpch_parquet_sf1/"

    }

    val sc = SparkContext.getOrCreate(sparkConf)
    val session = SparkSession.builder().getOrCreate();

    val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sc)

    //    val path = "/Users/yawen/Documents/Scala/tpch_parquet_sf1/lineitem.parquet"
    //    val path = "../CS422-Project2/src/main/resources/tpch_parquet_sf1/lineitem.parquet"
    //    val lineitem = sqlContext.read.load(path)

    //    val inputFile = "../lineitem.tbl"
    //    val input = new File(getClass.getResource(inputFile).getFile).getPath

    //    val customSchema = StructType(Array(
    //      StructField("l_orderkey", LongType, true),
    //      StructField("l_partkey", LongType, true),
    //      StructField("l_suppkey", LongType, true),
    //      StructField("l_linenumber", IntegerType, true),
    //      StructField("l_quantity", DoubleType, true),
    //      StructField("l_extendedprice", DoubleType, true),
    //      StructField("l_discount", DoubleType, true),
    //      StructField("l_tax", DoubleType, true),
    //      StructField("l_returnflag", StringType, true),
    //      StructField("l_linestatus", StringType, true),
    //      StructField("l_shipdate", DateType, true),
    //      StructField("l_commitdate", DateType, true),
    //      StructField("l_receiptdate", DateType, true),
    //      StructField("l_shipinstruct", StringType, true),
    //      StructField("l_shipmode", StringType, true),
    //      StructField("l_comment", StringType, true)))

    //    val lineitem = sqlContext.read
    //      .format("com.databricks.spark.csv")
    //      .option("header", "false")
    //      .option("delimiter", "|")
    //      .schema(customSchema)
    //      .load(input)

    val lineitem = sqlContext.read.load(path + "lineitem.parquet")
    val customer = sqlContext.read.load(path + "customer.parquet")
    val nation = sqlContext.read.load(path + "nation.parquet")
    val order = sqlContext.read.load(path + "order.parquet")
    val part = sqlContext.read.load(path + "part.parquet")
    val partsupp = sqlContext.read.load(path + "partsupp.parquet")
    val region = sqlContext.read.load(path + "region.parquet")
    val supplier = sqlContext.read.load(path + "supplier.parquet")

    var desc_ = new Description
    desc_.e = 0.01
    desc_.ci = 0.95

    desc_.lineitem = lineitem
    desc_.customer = customer
    desc_.nation = nation
    desc_.customer = customer
    desc_.part = part
    desc_.orders = order
    desc_.partsupp = partsupp
    desc_.region = region
    desc_.supplier = supplier

//    val sampling = Sampler.sample(desc_.lineitem, 600000000, desc_.e, desc_.ci)
//    desc_.samples = sampling._1
//    desc_.sampleDescription = sampling._2

    // Execute first query
    val sampleResult1 = Executor.execute_Q1(desc_, session, List(90)) // to "1998-09-02"
    sampleResult1.show()

    val sampleResult3 = Executor.execute_Q3(desc_, session, List("BUILDING", "1995-03-15"))
    sampleResult3.show()

    val sampleResult5 = Executor.execute_Q5(desc_, session, List("ASIA", "1995-01-01"))
    sampleResult5.show()

    val sampleResult6 = Executor.execute_Q6(desc_, session, List("1994-01-01", 0.06, 24))
    sampleResult6.show()

    val sampleResult7 = Executor.execute_Q7(desc_, session, List("FRANCE", "GERMANY"))
    sampleResult7.show()

    val sampleResult9 = Executor.execute_Q9(desc_, session, List("green"))
    sampleResult9.show()

    val sampleResult10 = Executor.execute_Q10(desc_, session, List("1993-10-01"))
    sampleResult10.show()

    val sampleResult11 = Executor.execute_Q11(desc_, session, List("GERMANY", 0.0001))
    sampleResult11.show()

    val sampleResult12 = Executor.execute_Q12(desc_, session, List("MAIL", "SHIP", "1994-01-01"))
    sampleResult12.show()

    val sampleResult17 = Executor.execute_Q17(desc_, session, List("Brand#23", "MED BOX"))
    sampleResult17.show()

    val sampleResult18 = Executor.execute_Q18(desc_, session, List(300))
    sampleResult18.show()

    val sampleResult19 = Executor.execute_Q19(desc_, session, List("Brand#12", "Brand#23", "Brand#34", 1, 10, 20))
    sampleResult19.show()

    val sampleResult20 = Executor.execute_Q20(desc_, session, List("forest", "1994-01-01", "CANADA"))
    sampleResult20.show()

  }

}
