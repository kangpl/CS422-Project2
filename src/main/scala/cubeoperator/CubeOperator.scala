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
      case "AVG" => rdd.map(row => (index.map(i => row(i)).mkString("_"), (row.getInt(indexAgg).toDouble, 1.0)))
      case _ => rdd.map(row => (index.map(i => row(i)).mkString("_"), row.getInt(indexAgg).toDouble))
    }

    //MRCombine & MRSpreadReduce
    val groupingReduce = agg match {
      case "AVG" => groupingMap.asInstanceOf[RDD[(String, (Double, Double) )]].
                    reduceByKey((left, right) => (left._1 + right._1, left._2 + right._2), reducers)
      case "MAX" => groupingMap.asInstanceOf[RDD[(String, Double)]].
                reduceByKey((left, right) => if (left > right) left else right , reducers)
      case "MIN" => groupingMap.asInstanceOf[RDD[(String, Double)]].
                reduceByKey((left, right) => if (left < right) left else right , reducers) 
      case _ => groupingMap.asInstanceOf[RDD[(String, Double)]].
                reduceByKey(_ + _ , reducers) 
    }
    
    //MRAssembleMapper
    val partialMap = for {
      row <- groupingReduce
      num <- 0 to groupingAttributes.length
      partialCell <- row._1.split("_").combinations(num)
    } yield(partialCell.mkString("_"), row._2)
    
    //MRCombine & MRAssembleReducer
    val cuboids = agg match{
      case "AVG" => partialMap.asInstanceOf[RDD[(String, (Double, Double))]].
                    reduceByKey((left, right) => ((left._1 + right._1), (left._2 + right._2)), reducers).
                    mapValues{case(sum, count) => sum / count}
      case "MAX" => partialMap.asInstanceOf[RDD[(String, Double)]].
                reduceByKey((left, right) => if (left > right) left else right , reducers)
      case "MIN" => partialMap.asInstanceOf[RDD[(String, Double)]].
                reduceByKey((left, right) => if (left < right) left else right , reducers) 
      case _ => partialMap.asInstanceOf[RDD[(String, Double)]].
                reduceByKey(_ + _ , reducers)                
    }
    
    cuboids
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    //TODO naive algorithm for cube computation
    null
  }

}
