package cubeoperator

import org.apache.spark.rdd.RDD

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    //TODO Task 1
    //MRSpread
    //MRSpreadMapper
    val groupingMap = agg match {
      case "COUNT" => rdd.map(row => (index.map(i => row(i)).mkString("_"), 1.0))
      case "AVG"   => rdd.map(row => (index.map(i => row(i)).mkString("_"), (row.getInt(indexAgg).toDouble, 1.0)))
      case _       => rdd.map(row => (index.map(i => row(i)).mkString("_"), row.getInt(indexAgg).toDouble))
    }

    //MRCombine & MRSpreadReduce
    val groupingReduce = agg match {
      case "AVG" => groupingMap.asInstanceOf[RDD[(String, (Double, Double))]].
        reduceByKey((left, right) => (left._1 + right._1, left._2 + right._2), reducers)
      case "MAX" => groupingMap.asInstanceOf[RDD[(String, Double)]].
        reduceByKey((left, right) => if (left > right) left else right, reducers)
      case "MIN" => groupingMap.asInstanceOf[RDD[(String, Double)]].
        reduceByKey((left, right) => if (left < right) left else right, reducers)
      case _ => groupingMap.asInstanceOf[RDD[(String, Double)]].
        reduceByKey(_ + _, reducers)
    }

    //MRAssembleMapper
    val partialMap = for {
      row <- groupingReduce
      num <- 0 to groupingAttributes.length
      partialCell <- row._1.split("_").combinations(num)
    } yield (partialCell.mkString("_"), row._2)

    //MRCombine & MRAssembleReducer
    val cuboids = agg match {
      case "AVG" => partialMap.asInstanceOf[RDD[(String, (Double, Double))]].
        reduceByKey((left, right) => ((left._1 + right._1), (left._2 + right._2)), reducers).
        mapValues { case (sum, count) => sum / count }
      case "MAX" => partialMap.asInstanceOf[RDD[(String, Double)]].
        reduceByKey((left, right) => if (left > right) left else right, reducers)
      case "MIN" => partialMap.asInstanceOf[RDD[(String, Double)]].
        reduceByKey((left, right) => if (left < right) left else right, reducers)
      case _ => partialMap.asInstanceOf[RDD[(String, Double)]].
        reduceByKey(_ + _, reducers)
    }

    cuboids
  }

  def avgFunc: ((Double, Double), (Double, Double)) => (Double, Double) = {
    (pair1: (Double, Double), pair2: (Double, Double)) => (pair1._1 + pair2._1, pair1._2 + pair2._2)
  }

  def aggFunc(agg: String): (Double, Double) => Double = {
    if (agg == "MAX") (value1: Double, value2: Double) => if (value1 > value2) value1 else value2
    else if (agg == "MIN") (value1: Double, value2: Double) => if (value1 < value2) value1 else value2
    else (value1: Double, value2: Double) => (value1 + value2)
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)
    val indexValues = (0 to index.length).toList

    // MAP(e) - get all possible combinations of the groupingAttrib - AggValue
    val mappedAttrib: RDD[Any] = rdd.flatMap(
      row => indexValues.flatMap(
        indexVal => index.map(
          i => row(i) // get all values of the corresponding index
        ).combinations(indexVal).toList.map( // list.comb(2) -> tuples of (1, 2) (1, 3) (2, 3)
            region =>
              if (agg == "COUNT") (region.mkString("_"), 1.toDouble) // val1_val2_val3, 1
              else if (agg == "AVG") (region.mkString("_"), (row.getInt(indexAgg).toDouble, 1.toDouble))
              else (region.mkString("_"), row.getInt(indexAgg).toDouble) // getInt(indexAgg) => get saved value from the rdd
          )))

    // Reduce
    val reducedAttrib: RDD[(String, Double)] = {
      if (agg == "AVG") mappedAttrib.asInstanceOf[RDD[(String, (Double, Double))]]
        .reduceByKey(avgFunc, reducers)
        .mapValues { case (sum, count) => sum / count }
      else mappedAttrib.asInstanceOf[RDD[(String, Double)]]
        .reduceByKey(aggFunc(agg), reducers)
    }

    reducedAttrib

  }

}
