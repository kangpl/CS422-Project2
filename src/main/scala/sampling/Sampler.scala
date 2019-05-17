package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.HashPartitioner
//import scala.util.control.Breaks._
//import org.apache.spark.sql.functions._
//import scala.math._

object Sampler {
  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    // TODO: implement
    val schema = lineitem.schema.map(x => x.name)
    val aggColumn = "l_extendedprice"
    val z_table = List((0.90, 1.645), (0.91, 1.70), (0.92, 1.75), (0.93, 1.81), (0.94, 1.88),
      (0.95, 1.96), (0.96, 2.05), (0.97, 2.17), (0.98, 2.33), (0.99, 2.575))
    var z = 2.575
    var i = 0
    var done = false
    while (i < z_table.size && !done) {
      if (ci <= z_table(i)._1) {
        z = z_table(i)._2
        done = true
      }
      i += 1
    }
    print("this is the z value: ", z, "according to confidence interval:", ci)

    // total query column sets appear in TPC-H queries
    val totalQcs = List(
      List("l_returnflag", "l_linestatus", "l_shipdate"), //List(8, 9, 10) Q1
      List("l_orderkey", "l_shipdate"), //List(0, 10) Q3
      List("l_orderkey", "l_suppkey"), //List(0, 2) Q5
      List("l_quantity", "l_discount", "l_shipdate"), //List(4, 6, 10) Q6
      List("l_orderkey", "l_suppkey", "l_shipdate"), //List(0, 2, 10) Q7
      List("l_orderkey", "l_partkey", "l_suppkey"), //List(0, 1, 2) Q9
      List("l_orderkey", "l_returnflag"), //List(0, 8) Q10
      List("l_orderkey", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipmode"), //List(0, 10, 11, 12, 14) Q11
      List("l_partkey", "l_quantity"), //List(1, 4) Q12
      List("l_orderkey", "l_quantity"), //List(0, 4) Q17
      List("l_partkey", "l_quantity", "l_shipinstruct", "l_shipmode"), //List(1, 4, 13, 14) Q18
      List("l_partkey", "l_suppkey", "l_shipdate")) //List(1, 2, 10) Q19
    val totalQcsIndex = totalQcs.map(qcs => qcs.map(q => schema.indexOf(q)))

    // useful query column sets after hardcode
    val usefulQcs = List(
      List(List("l_returnflag", "l_linestatus", "l_shipdate"), List("Q1")), //List(8, 9, 10)          //0.64%   /// Q1
      List(List("l_orderkey", "l_returnflag"), List("Q10")), //List(0, 8)              //34.49%  /// Q10
      List(List("l_partkey", "l_quantity"), List("Q12")), //List(1, 4)              //75.18%  /// Q12
      List(List("l_quantity", "l_discount", "l_shipdate"), List("Q6"))) //List(4, 6, 10)          //80.64%  /// Q6
    val usefulQcsIndex = usefulQcs.map(qcs => qcs(0).map(q => schema.indexOf(q)))
    val attrIndex = schema.indexOf(aggColumn)

    // according datatype to estimate the storage space of one tuple
    val rowBytes = lineitem.schema.map(x => x.dataType.defaultSize).reduce((a, b) => a + b)
    var storageBudgetTuples = storageBudgetBytes / rowBytes
    println("# tuples can be stored: ", storageBudgetTuples)

    // calculate absolute error according to relative error and sum of l_extendedprice
//    val sumValue = lineitem.agg(functions.sum(aggColumn)).first.get(0).asInstanceOf[java.math.BigDecimal].doubleValue()
    val sumValue = lineitem.agg(functions.sum(aggColumn)).first.getDouble(0)
    val errorBound = sumValue * e

    var haveStorageBudget = true
    i = 0
    var stratifiedSampleList = List[RDD[Row]]()
    var qcsIndicator = List[String]()
    while (i < usefulQcs.size && haveStorageBudget) {
      //get each stratum size and variance for specific qcs
      val dfAgg = lineitem.groupBy(usefulQcs(i)(0).head, usefulQcs(i)(0).tail: _*)
        .agg(functions.count(aggColumn), functions.var_pop(aggColumn))
        .select("count(" + aggColumn + ")", "var_pop(" + aggColumn + ")")

      //calculate magicK for specific qcs
      val magicK_samplesize = magicKSearch(dfAgg, aggColumn, z, errorBound)
      val magicK = magicK_samplesize._1
      val sampleSize = magicK_samplesize._2
      print("\nthis is the final magick: ", magicK, "eatimate sample size: ", sampleSize)
      
      //check whether have storage budget
      if (sampleSize < storageBudgetTuples) {
        storageBudgetTuples = storageBudgetTuples - sampleSize.toLong
        println("\nremained # tuples can be stored: ", storageBudgetTuples)
      } else {
        print("not have enough storage budget")
        haveStorageBudget = false
      }
      
      if(haveStorageBudget){
        // do scalable simple random sampling
        val lineitemRdd = lineitem.rdd
        
        val qcsWithKey = lineitemRdd.map(row => (usefulQcsIndex(i).map(x => row(x)).mkString("_"), row)).groupByKey
        val stratifiedSample = qcsWithKey.map(x => (x._1, (x._2.size, x._2))).flatMap(x => ScaSRS(x._2, magicK))
  
        println("real stratified sample size: ", stratifiedSample.count)
        println("# groups:", qcsWithKey.keys.distinct.count.toInt)
        
        stratifiedSampleList = stratifiedSampleList :+ stratifiedSample
        qcsIndicator = qcsIndicator ++ usefulQcs(i)(1)
        i += 1
      }
    }
    print("\nsampled qcs: ", qcsIndicator)
    (stratifiedSampleList, qcsIndicator)
  }

  def magicKSearch(dfAgg: DataFrame, aggColumn: String, z: Double, errorBound: Double): (Double, Double) = {
    var minStrataSize = 2.0
    var maxStrataSize = dfAgg.agg(functions.max("count(" + aggColumn + ")")).first.getLong(0).toDouble
    val rddNewAgg = dfAgg.rdd.map(row => Row(row.getLong(0).toDouble, row.getDouble(1)))
    var magicK = 0.0
    var sampleSize = 0.0
    
    var foundMinK = false

    while (!foundMinK) {
      var satisfied = false
      var K = scala.math.floor((minStrataSize + maxStrataSize) / 2)
      println()
      print("maxStrataSize: ", maxStrataSize)
      print("minStrataSize: ", minStrataSize)
      print("K: ", K)

      val estimateVar = rddNewAgg.map(row => calSizeTimesVar(row, K)).reduce(_+_)
      val estimateError = scala.math.sqrt(estimateVar) * z
      print("estimateError: ", estimateError, "  defined errorBound", errorBound)
      if (estimateError <= errorBound) {
        print("Satisfied")
        maxStrataSize = K
        satisfied = true
      } else {
        minStrataSize = K + 1
        print("Not satisfied")
      }

      if ((maxStrataSize == minStrataSize) && satisfied) {
        magicK = K
        foundMinK = true
        sampleSize = rddNewAgg.map(row => calSize(row, magicK)).reduce(_+_)
      }

      // avoid loop
      if ((maxStrataSize < minStrataSize) && !satisfied) {
        magicK = maxStrataSize
        foundMinK = true
        sampleSize = rddNewAgg.map(row => calSize(row, magicK)).reduce(_+_)
      }

      if ((maxStrataSize < minStrataSize) && satisfied) {
        magicK = minStrataSize
        foundMinK = true
        sampleSize = rddNewAgg.map(row => calSize(row, magicK)).reduce(_+_)
      }
    }
    return (magicK, sampleSize)
  }

  def calSizeTimesVar(row: Row, K: Double): Double = {
    var stratumsampleSize = K
    val stratumSize = row.getDouble(0)
    if (stratumSize < K) stratumsampleSize = stratumSize
    val error = scala.math.pow(stratumSize, 2) * row.getDouble(1) / stratumsampleSize
    return error
  }
  
  def calSize(row: Row, K: Double): Double = {
    var stratumsampleSize = K
    val stratumSize = row.getDouble(0)
    if (stratumSize < K) stratumsampleSize = stratumSize
    return stratumsampleSize
  }

  def ScaSRS(size_stratum: (Int, Iterable[Row]), K: Double): Iterable[Row] = {
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
