package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.HashPartitioner
import scala.math._

object Sampler {
  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double, ctx: SparkContext): (List[RDD[_]], _) = {
    // TODO: implement
    val rowBytes = lineitem.schema.map(x => x.dataType.defaultSize).reduce((a, b) => a+b)
    
    val schema = lineitem.schema.map(x => x.name)
    val totalQcs = List(
        List("l_returnflag", "l_linestatus", "l_shipdate"),                                //List(8, 9, 10)
        List("l_orderkey", "l_shipdate"),                                                  //List(0, 10)
        List("l_orderkey", "l_suppkey"),                                                   //List(0, 2)
        List("l_quantity", "l_discount", "l_shipdate"),                                    //List(4, 6, 10) 
        List("l_orderkey", "l_suppkey", "l_shipdate"),                                     //List(0, 2, 10)
        List("l_orderkey", "l_partkey", "l_suppkey"),                                      //List(0, 1, 2)
        List("l_orderkey", "l_returnflag"),                                                //List(0, 8)
        List("l_orderkey", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipmode"),   //List(0, 10, 11, 12, 14)
        List("l_partkey", "l_quantity"),                                                   //List(1, 4)
        List("l_orderkey", "l_quantity"),                                                  //List(0, 4)
        List("l_partkey", "l_quantity", "l_shipinstruct", "l_shipmode"),                   //List(1, 4, 13, 14)
        List("l_partkey", "l_suppkey", "l_shipdate"))                                      //List(1, 2, 10)   
    val totalQcsIndex = totalQcs.map(qcs => qcs.map(q => schema.indexOf(q)))
    
    val lineitemRdd = ctx.parallelize(lineitem.take(100000))
//    val lineitemRdd = lineitem.rdd
    //calculate minimum k for each query column set
    val qcsWithKey = lineitemRdd.map(row => (totalQcsIndex(1).map(x => row(x)).mkString("_"), row))  
//    var minStrataSize = 1
//    var maxStrataSize = qcsWithKey.groupByKey.map(x => x._2.size).max
//    while(minStrataSize <= maxStrataSize){
//      var k = (minStrataSize + maxStrataSize) / 2
//      val groupedQcs = qcsWithKey.groupByKey
//      val newGroupedQcs = groupedQcs.map( x => (x._1, x._2, calSampleSize(x._2, k)))
//      val totalSampleSize = newGroupedQcs.map(x => x._3).reduce((a, b) => a + b)
//      newGroupedQcs.map( x => calculate(x._2, x._3, totalSampleSize) )  
//    }
    val K = 1
    val stratifiedSample = qcsWithKey.groupByKey.map(x => (x._1, (x._2.size, x._2))).flatMap(x =>ScaSRS(x._2, K))
    print(stratifiedSample.count())
    print(qcsWithKey.keys.distinct.count.toInt)
    null
  }
  
  def ScaSRS(size_stratum: (Int, Iterable[Row]), K: Int) : Iterable[Row] = {
    val sigma = 0.00000001
    val stratumSize = size_stratum._1
    val stratum = size_stratum._2.toIterator
    val r = scala.util.Random
    
    if (stratumSize < K) stratum.toIterable
    else {
      val p = K.toDouble / stratumSize.toDouble
      val gamma1 = -1.0 * (log(sigma) / stratumSize)
      val gamma2 = -1.0 * (2 * log(sigma) / 3 / stratumSize)
      val q1 = min(1, p + gamma1 + sqrt(gamma1 * gamma1 + 2 * gamma1 * p))
      val q2 = max(0, p + gamma2 - sqrt(gamma2 * gamma2 + 3 * gamma2 * p))
      
      var l = 0
      var waitlist = List[(Double, Row)]()
      var res = List[Row]()
      
      while(stratum.toIterator.hasNext) {
        val nextRow = stratum.next
        val Xj = r.nextDouble
        if(Xj < q2) { 
          res = res :+ nextRow
          l += 1
        }
        else if(Xj < q1){
          waitlist = waitlist :+ (Xj, nextRow)
        }
      }
      
      //select the smallest pn-l items from waitlist
      waitlist = waitlist.sortBy(_._1)
      if(p * stratumSize - l > 0) res = res ++ waitlist.take(ceil((p * stratumSize)).toInt - l).map(_._2)
      res
    }
  }
}
