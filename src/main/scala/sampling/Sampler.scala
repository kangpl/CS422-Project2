package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.HashPartitioner
//import org.apache.spark.sql.functions._
//import scala.math._

object Sampler {
  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    // TODO: implement
    val schema = lineitem.schema.map(x => x.name)

    // total query column sets appear in TPC-H queries
    val totalQcs = List(
      List("l_returnflag", "l_linestatus", "l_shipdate"), //List(8, 9, 10)
      List("l_orderkey", "l_shipdate"), //List(0, 10)
      List("l_orderkey", "l_suppkey"), //List(0, 2)
      List("l_quantity", "l_discount", "l_shipdate"), //List(4, 6, 10)
      List("l_orderkey", "l_suppkey", "l_shipdate"), //List(0, 2, 10)
      List("l_orderkey", "l_partkey", "l_suppkey"), //List(0, 1, 2)
      List("l_orderkey", "l_returnflag"), //List(0, 8)
      List("l_orderkey", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipmode"), //List(0, 10, 11, 12, 14)
      List("l_partkey", "l_quantity"), //List(1, 4)
      List("l_orderkey", "l_quantity"), //List(0, 4)
      List("l_partkey", "l_quantity", "l_shipinstruct", "l_shipmode"), //List(1, 4, 13, 14)
      List("l_partkey", "l_suppkey", "l_shipdate")) //List(1, 2, 10)
    val totalQcsIndex = totalQcs.map(qcs => qcs.map(q => schema.indexOf(q)))

    // useful query column sets after hardcode
    val usefulQcs = List(
      List("l_returnflag", "l_linestatus", "l_shipdate"), //List(8, 9, 10)          //0.64%   ///
      List("l_quantity", "l_discount", "l_shipdate"), //List(4, 6, 10)          //80.64%  ///
      List("l_orderkey", "l_returnflag"), //List(0, 8)              //34.49%  ///
      List("l_partkey", "l_quantity")) //List(1, 4)              //75.18%  ///
    val usefulQcsIndex = usefulQcs.map(qcs => qcs.map(q => schema.indexOf(q)))
    val attrIndex = schema.indexOf("l_extendedprice")

    // according datatype to estimate the storage space of one tuple
    val rowBytes = lineitem.schema.map(x => x.dataType.defaultSize).reduce((a, b) => a + b)
    val storageBudgetTuples = storageBudgetBytes / rowBytes

    // calculate absolute error according to relative error and sum of l_extendedprice
    val sumValue = lineitem.agg(functions.sum("l_extendedprice")).first.getDouble(0)
    val errorBound = sumValue * e

    //    val lineitemRdd = ctx.parallelize(lineitem.take(100000))
    val lineitemRdd = lineitem.rdd

    //calculate minimum k for each query column set
    val dfAgg = lineitem.groupBy("l_returnflag", "l_linestatus", "l_shipdate")
      .agg(functions.count("l_extendedprice"), functions.var_pop("l_extendedprice"))
      .select("count(l_extendedprice)", "var_pop(l_extendedprice)")
    val rddAgg = dfAgg.rdd
    val rddNewAgg = rddAgg.map(row => Row(row.getLong(0).toDouble, row.getDouble(1)))

    var minStrataSize = 1.0
    var maxStrataSize = dfAgg.agg(functions.max("count(l_extendedprice)")).first.getLong(0).toDouble
    var magicK = 0.0

    print("original maxStrataSize: ", maxStrataSize)
    println("original minStrataSize: ", minStrataSize)

    var foundMinK = false

    while (!foundMinK) {
      var satisfied = false
      var K = scala.math.floor((minStrataSize + maxStrataSize) / 2)
      print("K: ", K)
      val stratumError = rddNewAgg.map(row => calError(row, K))
      val stratumSum = stratumError.reduce(_ + _)
      val totalError = scala.math.sqrt(stratumSum) * 1.96
      print("totalError: ", totalError, "errorBound", errorBound)
      if (totalError <= errorBound) {
        print("Satisfied")
        maxStrataSize = K
        satisfied = true
      } else {
        minStrataSize = K + 1
        print("Not satisfied")
      }

      print("maxStrataSize: ", maxStrataSize)
      println("minStrataSize: ", minStrataSize)

      if ((maxStrataSize == minStrataSize) && satisfied) {
        magicK = K
        foundMinK = true
      }

      // avoid loop
      if ((maxStrataSize < minStrataSize) && !satisfied) {
        magicK = maxStrataSize
        foundMinK = true
      }
    }
    print(magicK)
    //    val K = 2
    //    val qcsWithKey = lineitemRdd.map(row => (usefulQcsIndex(0).map(x => row(x)).mkString("_"), row)).groupByKey
    //    val stratifiedSample = qcsWithKey.map(x => (x._1, (x._2.size, x._2))).flatMap(x =>ScaSRS(x._2, K))
    //    print(stratifiedSample.count())
    //    print(qcsWithKey.keys.distinct.count.toInt)
    null
  }

  def calError(row: Row, K: Double): Double = {
    var sampleSize = K
    val stratumSize = row.getDouble(0)
    if (stratumSize < K) sampleSize = stratumSize
    val error = scala.math.pow(stratumSize, 2) * row.getDouble(1) / sampleSize
    return error
  }

  def ScaSRS(size_stratum: (Int, Iterable[Row]), K: Int): Iterable[Row] = {
    val sigma = 0.00005
    val stratumSize = size_stratum._1
    val stratum = size_stratum._2.toIterator
    val r = scala.util.Random

    if (stratumSize < K) stratum.toIterable
    else {
      val p = K.toDouble / stratumSize.toDouble
      val gamma1 = -1.0 * (scala.math.log(sigma) / stratumSize)
      val gamma2 = -1.0 * (2 * scala.math.log(sigma) / 3 / stratumSize)
      val q1 = scala.math.min(1, p + gamma1 + scala.math.sqrt(gamma1 * gamma1 + 2 * gamma1 * p))
      val q2 = scala.math.max(0, p + gamma2 - scala.math.sqrt(gamma2 * gamma2 + 3 * gamma2 * p))

      var l = 0
      var waitlist = List[(Double, Row)]()
      var res = List[Row]()

      while (stratum.hasNext) {
        val nextRow = stratum.next
        val Xj = r.nextDouble
        if (Xj < q2) {
          res = res :+ nextRow
          l += 1
        } else if (Xj < q1) {
          waitlist = waitlist :+ (Xj, nextRow)
        }
      }

      //select the smallest pn-l items from waitlist
      waitlist = waitlist.sortBy(_._1)
      if (p * stratumSize - l > 0) res = res ++ waitlist.take(scala.math.ceil((p * stratumSize)).toInt - l).map(_._2)
      res
    }
  }
}
