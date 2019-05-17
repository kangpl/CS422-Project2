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
      path = "/cs422-data/tpch/sf100/parquet/"
    } else {
      sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
      path = "/Users/yawen/Documents/Scala/tpch_parquet_sf1/"

    }

    val sc = SparkContext.getOrCreate(sparkConf)
    val session = SparkSession.builder().getOrCreate();

    val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sc)

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

    val sampling = Sampler.sample(desc_.lineitem, 600000000, desc_.e, desc_.ci)
    desc_.samples = sampling._1
    desc_.sampleDescription = sampling._2

    // Execute first query
    println("Q1")
    val sampleResult1 = Executor.execute_Q1(desc_, session, List(90)) // to "1998-09-02"
    sampleResult1.show()

    println("Q3")
    val sampleResult3 = Executor.execute_Q3(desc_, session, List("BUILDING", "1995-03-15"))
    sampleResult3.show()

    println("Q5")
    val sampleResult5 = Executor.execute_Q5(desc_, session, List("ASIA", "1995-01-01"))
    sampleResult5.show()

    println("Q6")
    val sampleResult6 = Executor.execute_Q6(desc_, session, List("1994-01-01", 0.06, 24))
    sampleResult6.show()

    println("Q7")
    val sampleResult7 = Executor.execute_Q7(desc_, session, List("FRANCE", "GERMANY"))
    sampleResult7.show()

    println("Q9")
    val sampleResult9 = Executor.execute_Q9(desc_, session, List("green"))
    sampleResult9.show()

    println("Q10")
    val sampleResult10 = Executor.execute_Q10(desc_, session, List("1993-10-01"))
    sampleResult10.show()

    println("Q11")
    val sampleResult11 = Executor.execute_Q11(desc_, session, List("GERMANY", 0.0001))
    sampleResult11.show()

    println("Q12")
    val sampleResult12 = Executor.execute_Q12(desc_, session, List("MAIL", "SHIP", "1994-01-01"))
    sampleResult12.show()

    println("Q17")
    val sampleResult17 = Executor.execute_Q17(desc_, session, List("Brand#23", "MED BOX"))
    sampleResult17.show()

    println("Q18")
    val sampleResult18 = Executor.execute_Q18(desc_, session, List(300))
    sampleResult18.show()

    println("Q19")
    val sampleResult19 = Executor.execute_Q19(desc_, session, List("Brand#12", "Brand#23", "Brand#34", 1, 10, 20))
    sampleResult19.show()

    println("Q20")
    val sampleResult20 = Executor.execute_Q20(desc_, session, List("forest", "1994-01-01", "CANADA"))
    sampleResult20.show()

  }

}