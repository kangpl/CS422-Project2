package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

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
    //    var homeArray: Array[String] = Array(null)
    //    var rddResult: RDD[(String, String)] = null

    val get_partition = rdd.map(
      p => (
        p(attrIndex).toString,
        anchors.map(
          a =>
            edit_distance(p(attrIndex), a._1(attrIndex))).zipWithIndex.min))

    //(Aaaaeldin M.sHsfez,(13,0))
    //      get_partition.foreach(x => x._2.foreach(y => println(y)))

    //    val SimilarityMapper = anchors.map(
    //        a =>
    //          get_partition.map(
    //              row =>
    //                if (row._2._2 == a._2)
    //                  (a._2, "home", (row._1, "home"))
    //                else if ((row._2._2 < a._2) ^ ((row._2._2 + a._2) % 2 == 1) && edit_distance(row._1, a._1(attrIndex)) < row._2._1 + 2 * distThreshold)
    //                  (a._2, "outer", (row._1, "outer"))
    //                else null
    //      ).filter(x => x != null)
    //      .sortBy(_._1))
    //
    //
    //
    //    SimilarityMapper.foreach(x => x.foreach(z=> println(z)))
    //

    val SimilarityMapper = get_partition.flatMap(
      row =>
        anchors.map(
          a =>
            if (row._2._2 == a._2)
              (a._2, ("home", row._1))
            else if ((row._2._2 < a._2) ^ ((row._2._2 + a._2) % 2 == 1) && edit_distance(row._1, a._1(attrIndex)) < row._2._1 + 2 * distThreshold)
              (a._2, ("outer", row._1))
            else null)).filter(x => x != null).sortBy(line => (line._1, line._2._1), ascending = true)

    //    SimilarityMapper.foreach(x => println(x))

    val SimilarityReducer = SimilarityMapper.reduceByKey(customFunc, 4)

        SimilarityReducer.count()

    //     val SimPair = SimilarityMapper.map( partition => partition.map( x =>
    //       if (x._2._2 == "home") {
    //         for(hp <- homeArray) {
    //           if(edit_distance(hp, x._2._1) < distThreshold)
    //         }
    //         homeArray :+ x._2._1
    //       }
    //
    null
  }

  def customFunc: ((String, String), (String, String)) => (String, String) = {
    (pair1: (String, String), pair2: (String, String)) =>
      println(pair1._1 + " " + pair2._1)
      pair1

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

