package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.HashPartitioner

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class SimilarityJoin(numAnchors: Int, distThreshold: Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("SimilarityJoin")
  var rdd: RDD[String] = null

  /*
   * this method gets as input a dataset and the index of an attribute
   * of the dataset, and returns the result of the similarity self join on
   * that attribute.
   * */
  def similarity_join(dataset: Dataset, attrIndex: Int): RDD[(String, String)] = {
    val rdd = dataset.getRDD
    val anchors = rdd.takeSample(false, numAnchors).zipWithIndex

    val get_partition = rdd.map(
      p => (
        p(attrIndex).toString,
        anchors.map(
          a =>
            edit_distance(p(attrIndex), a._1(attrIndex))).zipWithIndex.min))

    val SimilarityMapper = get_partition.flatMap(
      row =>
        anchors.map(
          a =>
            if (row._2._2 == a._2)
              (a._2, ("home", row._1))
            else if (((row._2._2 < a._2) ^ ((row._2._2 + a._2) % 2 == 1)) && edit_distance(row._1, a._1(attrIndex)) <= row._2._1 + 2 * distThreshold)
              (a._2, ("outer", row._1))
            else null)).filter(x => x != null).sortBy(line => (line._1, line._2._1), ascending = true)
            
    val rddPartitioned = SimilarityMapper.partitionBy(new HashPartitioner(numAnchors))
        
    val result = {
      rddPartitioned.mapPartitionsWithIndex( (index, partitions) => {    
        val joinResult = local_simjoin(partitions)
        joinResult
      })
    }.repartition(1)
//    println(result.count())
//    result.foreach(x => println(x))
    result
  }
    
  def local_simjoin(cluster:Iterator[(Int, (String, String))]) : Iterator[(String, String)] = {
    var res = List[(String, String)]()
    var homelist = List[String]()  
        
    while(cluster.hasNext) {
      val point = cluster.next
      for(prevPoint <- homelist) {
        if(edit_distance(point._2._2, prevPoint) <= distThreshold)  {
          res = res :+ (point._2._2, prevPoint)
        }
      }
      if(point._2._1 == "home"){
          homelist = homelist :+ point._2._2
      }
    }
    
    res.iterator
  }

  def edit_distance(val1: Any, val2: Any): Int = {
    val str1 = val1.toString
    val str2 = val2.toString

    val lenStr1 = str1.length
    val lenStr2 = str2.length

    val d: Array[Array[Int]] = Array.ofDim(lenStr1 + 1, lenStr2 + 1)

    for (i <- 0 to lenStr1) d(i)(0) = i
    for (j <- 0 to lenStr2) d(0)(j) = j

    for (i <- 1 to lenStr1; j <- 1 to lenStr2) {
      val cost = if (str1(i - 1) == str2(j - 1)) 0 else 1

      d(i)(j) = List(
        d(i - 1)(j) + 1, // deletion
        d(i)(j - 1) + 1, // insertion
        d(i - 1)(j - 1) + cost // substitution
      ).min
    }

    d(lenStr1)(lenStr2)
  }
}

