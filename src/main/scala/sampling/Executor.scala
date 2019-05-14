package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SparkSession, Row }
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

object Executor {
  def execute_Q1(desc: Description, session: SparkSession, params: List[Any]) = {

    import session.implicits._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }

    var lineitem: DataFrame = null

    if (desc.samples.isEmpty) {
      println("Using whole dataframe")
      lineitem = desc.lineitem
      lineitem.show(20)
    } else {
      println("Using sample")
      lineitem = session.createDataFrame(desc.samples(0).asInstanceOf[RDD[Row]], desc.lineitem.schema)
      lineitem.show(20)
    }

    lineitem.filter($"l_shipdate" <= "1998-09-02")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(sum($"l_quantity"), sum($"l_extendedprice"),
        sum(decrease($"l_extendedprice", $"l_discount")),
        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
        avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
      .sort($"l_returnflag", $"l_linestatus")
  }

  def execute_Q3(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    assert(params.size == 2)
    // https://github.com/electrum/tpch-dbgen/blob/master/queries/3.sql
    // using:
    // params(0) as :1
    // params(1) as :2
  }

  def execute_Q5(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q6(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q7(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q9(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q10(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q11(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q12(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q17(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q18(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q19(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q20(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }
}
