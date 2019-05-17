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
    rdd = dataset.getRDD.map(row => row.getString(attrIndex))
    val anchors = rdd.takeSample(false, numAnchors).zipWithIndex

    val get_partition = rdd.map(
      p => (
        p,
        anchors.map(
          a =>
            edit_distance(p, a._1)).zipWithIndex.min))

    val SimilarityMapper = get_partition.flatMap(
      row =>
        anchors.map(
          a =>
            if (row._2._2 == a._2)
              (a._2, ("home", row._1))
            else if (((row._2._2 < a._2) ^ ((row._2._2 + a._2) % 2 == 1)) && edit_distance(row._1, a._1) <= row._2._1 + 2 * distThreshold)
              (a._2, ("outer", row._1))
            else null)).filter(x => x != null).sortBy(line => (line._1, line._2._1), ascending = true)
            
    val rddPartitioned = SimilarityMapper.partitionBy(new HashPartitioner(numAnchors))
    print("size: ", rddPartitioned.count())
        
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
          res = res :+ (prevPoint, point._2._2)
        }
      }
      if(point._2._1 == "home"){
          homelist = homelist :+ point._2._2
      }
    }
    
    res.iterator
  }


  // Code inspired from Rosetta Stone
  def edit_distance(s1: String, s2: String): Int = {            
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }
   
      @inline
      def minimum(i: Int*): Int = i.min
   
      for {j <- dist.indices.tail
           i <- dist(0).indices.tail} dist(j)(i) =
          if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
          else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
   
      dist(s2.length)(s1.length)
  }
}