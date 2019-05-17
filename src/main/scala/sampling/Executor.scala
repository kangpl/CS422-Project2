package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SparkSession, Row }
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf
import java.time._
import java.time.format._

object Executor {

  def calcDate(date: String, dateType: String, interval: Long, minus: Boolean): String = {
    val toDate = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val initialDate = LocalDate.parse(date, toDate)
    var returnDate = ""

    if (minus) {
      dateType match {
        case "day"   => returnDate = initialDate.minusDays(interval).toString()
        case "month" => returnDate = initialDate.minusMonths(interval).toString()
        case "year"  => returnDate = initialDate.minusYears(interval).toString()
      }
    } else {
      dateType match {
        case "day"   => returnDate = initialDate.plusDays(interval).toString()
        case "month" => returnDate = initialDate.plusMonths(interval).toString()
        case "year"  => returnDate = initialDate.plusYears(interval).toString()
      }
    }

    print("date: " + date + " ; final date: " + returnDate)
    returnDate
  }

  def execute_Q1(desc: Description, session: SparkSession, params: List[Any]) = {

    import session.implicits._

    var lineitem: DataFrame = null
    var proportion = 1.0

    if (desc.samples == null) {
      println("Using whole dataframe")
      lineitem = desc.lineitem
      //      lineitem.show(20)
    } else {
      println("Using sample")
      lineitem = session.createDataFrame(desc.samples(0).asInstanceOf[RDD[Row]], desc.lineitem.schema)
      //      lineitem.show(20)
    }

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val increase = udf { (x: Double, y: Double) => x * (1 + y) }
    val multitply = udf { (x: Double) => x * proportion }

    // Calculate true date
    val dateInterval: Long = params(0).toString().toLong
    val date: String = calcDate("1998-12-01", "day", dateInterval, true)

    val where_ = lineitem.filter($"l_shipdate" <= date)

    val grBy_ = where_.groupBy($"l_returnflag", $"l_linestatus")

    val result = grBy_.agg(
      sum($"l_quantity").as("sum_qty"),
      sum($"l_extendedprice").as("sum_base_price"),
      sum(decrease($"l_extendedprice", $"l_discount")).as("sum_disc_price"),
      sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")).as("sum_charge"),
      avg($"l_quantity").as("avg_qty"),
      avg($"l_extendedprice").as("avg_price"),
      avg($"l_discount").as("avg_disc"),
      count($"l_quantity").as("count_order"))

    result.sort($"l_returnflag", $"l_linestatus")

  }

  def execute_Q3(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    assert(params.size == 2)
    // https://github.com/electrum/tpch-dbgen/blob/master/queries/3.sql
    // using:
    // params(0) as :1
    // params(1) as :2
    import session.implicits._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val customFilter = desc.customer.filter($"c_mktsegment" === params(0))
    val orderFilter = desc.orders.filter($"o_orderdate" < params(1))
    val lineitemFilter = desc.lineitem.filter($"l_shipdate" > params(1))

    val where_ = customFilter.join(orderFilter, $"c_custkey" === orderFilter("o_custkey"))
      .select($"o_orderkey", $"o_orderdate", $"o_shippriority")
      .join(lineitemFilter, $"o_orderkey" === lineitemFilter("l_orderkey"))

    val select_ = where_.select(
      $"l_orderkey",
      decrease($"l_extendedprice", $"l_discount").as("part_revenue"),
      $"o_orderdate", $"o_shippriority")

    val grBy_ = select_.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
    val result = grBy_.agg(sum($"part_revenue").as("revenue"))

    result.sort($"revenue".desc, $"o_orderdate").limit(10)
  }

  def execute_Q5(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val rname: String = params(0).toString()
    val firstDate: String = params(1).toString()
    val secondDate = calcDate(firstDate, "year", 1, false)

    val orderFilter = desc.orders.filter($"o_orderdate" < firstDate && $"o_orderdate" >= secondDate)
    val regionFilter = desc.region.filter($"r_name" === rname)

    val where_ = regionFilter.join(desc.nation, $"r_regionkey" === desc.nation("n_regionkey"))
      .join(desc.supplier, $"n_nationkey" === desc.supplier("s_nationkey"))
      .join(desc.lineitem, $"s_suppkey" === desc.lineitem("l_suppkey"))
      .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey")
      
   val where2_ = where_.join(orderFilter, $"l_orderkey" === orderFilter("o_orderkey"))
      .join(desc.customer, $"o_custkey" === desc.customer("c_custkey") && $"s_nationkey" === desc.customer("c_nationkey"))
      .select($"n_name", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"n_name")
      .agg(sum($"value").as("revenue"))
      .sort($"revenue".desc)
  }

  def execute_Q6(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    var lineitem: DataFrame = null
    //    var proportion = 1.0
    //    var returnValue = _

    if (desc.samples == null) {
      println("Using whole dataframe")
      lineitem = desc.lineitem
      //      lineitem.show(20)
    } else {
      println("Using sample")
      lineitem = session.createDataFrame(desc.samples(0).asInstanceOf[RDD[Row]], desc.lineitem.schema)
      //      lineitem.show(20)
    }

    lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01" && $"l_discount" >= 0.05 && $"l_discount" <= 0.07 && $"l_quantity" < 24)
      .agg(sum($"l_extendedprice" * $"l_discount"))
  }

  def execute_Q7(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    val lineitem = desc.lineitem
    val order = desc.orders
    val partsupp = desc.partsupp
    val nation = desc.nation
    val supplier = desc.supplier
    val customer = desc.customer

    val getYear = udf { (x: String) => x.substring(0, 4) }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    
    val r_name: String = params(0).toString()
    val firstDate: String = params(1).toString()
    val secondDate: String = calcDate(firstDate, "year", 1, false)


    val fnation = nation.filter($"n_name" === "FRANCE" || $"n_name" === "GERMANY")
    val fline = lineitem.filter($"l_shipdate" >= "1995-01-01" && $"l_shipdate" <= "1996-12-31")

    val supNation = fnation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(fline, $"s_suppkey" === fline("l_suppkey"))
      .select($"n_name".as("supp_nation"), $"l_orderkey", $"l_extendedprice", $"l_discount", $"l_shipdate")

    fnation.join(customer, $"n_nationkey" === customer("c_nationkey"))
      .join(order, $"c_custkey" === order("o_custkey"))
      .select($"n_name".as("cust_nation"), $"o_orderkey")
      .join(supNation, $"o_orderkey" === supNation("l_orderkey"))
      .filter($"supp_nation" === "FRANCE" && $"cust_nation" === "GERMANY"
        || $"supp_nation" === "GERMANY" && $"cust_nation" === "FRANCE")
      .select($"supp_nation", $"cust_nation",
        getYear($"l_shipdate").as("l_year"),
        decrease($"l_extendedprice", $"l_discount").as("volume"))
      .groupBy($"supp_nation", $"cust_nation", $"l_year")
      .agg(sum($"volume").as("revenue"))
      .sort($"supp_nation", $"cust_nation", $"l_year")
  }

  def execute_Q9(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    val lineitem = desc.lineitem
    val order = desc.orders
    val partsupp = desc.partsupp
    val nation = desc.nation
    val supplier = desc.supplier
    val part = desc.part

    val getYear = udf { (x: String) => x.substring(0, 4) }
    val expr = udf { (x: Double, y: Double, v: Double, w: Double) => x * (1 - y) - (v * w) }

    val linePart = part.filter($"p_name".contains("green"))
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"))

    val natSup = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    linePart.join(natSup, $"l_suppkey" === natSup("s_suppkey"))
      .join(partsupp, $"l_suppkey" === partsupp("ps_suppkey")
        && $"l_partkey" === partsupp("ps_partkey"))
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"n_name", getYear($"o_orderdate").as("o_year"),
        expr($"l_extendedprice", $"l_discount", $"ps_supplycost", $"l_quantity").as("amount"))
      .groupBy($"n_name", $"o_year")
      .agg(sum($"amount"))
      .sort($"n_name", $"o_year".desc)

  }

  def execute_Q10(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    var lineitem: DataFrame = null
    var proportion = 1.0

    if (desc.samples == null) {
      println("Using whole dataframe")
      lineitem = desc.lineitem
      //      lineitem.show(20)
    } else {
      println("Using sample")
      lineitem = session.createDataFrame(desc.samples(0).asInstanceOf[RDD[Row]], desc.lineitem.schema)
      proportion = desc.sampleDescription.asInstanceOf[Double]
      //      lineitem.show(20)
    }

    val date = params(0)

    val order = desc.orders
    val customer = desc.customer
    val nation = desc.nation

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val multiply = udf { (x: Double) => x * proportion }

    val lineitemFilter = lineitem.filter($"l_returnflag" === "R")
    val orderFilter = order.filter($"o_orderdate" >= "1993-10-01" && $"o_orderdate" < "1994-01-01")

    orderFilter.join(customer, $"o_custkey" === customer("c_custkey"))
      .join(nation, $"c_nationkey" === nation("n_nationkey"))
      .join(lineitemFilter, $"o_orderkey" === lineitemFilter("l_orderkey"))
      .select($"c_custkey", $"c_name",
        decrease($"l_extendedprice", $"l_discount").as("volume"),
        $"c_acctbal", $"n_name", $"c_address", $"c_phone", $"c_comment")
      .groupBy($"c_custkey", $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"volume").as("revenue"))
      .sort($"revenue".desc)
      .limit(20)
  }

  def execute_Q11(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    val nation = desc.nation
    val supplier = desc.supplier
    val partsupp = desc.partsupp

    val mul = udf { (x: Double, y: Int) => x * y }
    val mul01 = udf { (x: Double) => x * 0.0001 }

    val tmp = nation.filter($"n_name" === "GERMANY")
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", mul($"ps_supplycost", $"ps_availqty").as("value"))
    // .cache()

    val sumRes = tmp.agg(sum("value").as("total_value"))

    tmp.groupBy($"ps_partkey").agg(sum("value").as("part_value"))
      .join(sumRes, $"part_value" > mul01($"total_value"))
      .sort($"part_value".desc)
  }

  def execute_Q12(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    val mul = udf { (x: Double, y: Double) => x * y }
    val highPriority = udf { (x: String) => if (x == "1-URGENT" || x == "2-HIGH") 1 else 0 }
    val lowPriority = udf { (x: String) => if (x != "1-URGENT" && x != "2-HIGH") 1 else 0 }

    var lineitem: DataFrame = null
    val order = desc.orders

    var proportion = 1.0

    if (desc.samples == null) {
      println("Using whole dataframe")
      lineitem = desc.lineitem
      //      lineitem.show(20)
    } else {
      println("Using sample")
      lineitem = session.createDataFrame(desc.samples(0).asInstanceOf[RDD[Row]], desc.lineitem.schema)
      proportion = desc.sampleDescription.asInstanceOf[Double]
      //      lineitem.show(20)
    }

    lineitem.filter((
      $"l_shipmode" === "MAIL" || $"l_shipmode" === "SHIP") &&
      $"l_commitdate" < $"l_receiptdate" &&
      $"l_shipdate" < $"l_commitdate" &&
      $"l_receiptdate" >= "1994-01-01" && $"l_receiptdate" < "1995-01-01")
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"l_shipmode", $"o_orderpriority")
      .groupBy($"l_shipmode")
      .agg(
        sum(highPriority($"o_orderpriority")).as("sum_highorderpriority"),
        sum(lowPriority($"o_orderpriority")).as("sum_loworderpriority"))
      .sort($"l_shipmode")
  }

  def execute_Q17(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    val mul02 = udf { (x: Double) => x * 0.2 }

    var lineitem: DataFrame = desc.lineitem
    var part = desc.part

    val flineitem = lineitem.select($"l_partkey", $"l_quantity", $"l_extendedprice")

    val fpart = part.filter($"p_brand" === "Brand#23" && $"p_container" === "MED BOX")
      .select($"p_partkey")
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")
    // select

    fpart.groupBy("p_partkey")
      .agg(mul02(avg($"l_quantity")).as("avg_quantity"))
      .select($"p_partkey".as("key"), $"avg_quantity")
      .join(fpart, $"key" === fpart("p_partkey"))
      .filter($"l_quantity" < $"avg_quantity")
      .agg(sum($"l_extendedprice") / 7.0)
  }

  def execute_Q18(desc: Description, session: SparkSession, params: List[Any]) = {

    import session.implicits._

    var lineitem: DataFrame = desc.lineitem
    var order = desc.orders
    var customer = desc.customer

    lineitem.groupBy($"l_orderkey")
      .agg(sum($"l_quantity").as("sum_quantity"))
      .filter($"sum_quantity" > 300)
      .select($"l_orderkey".as("key"), $"sum_quantity")
      .join(order, order("o_orderkey") === $"key")
      .join(lineitem, $"o_orderkey" === lineitem("l_orderkey"))
      .join(customer, customer("c_custkey") === $"o_custkey")
      .select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .agg(sum("l_quantity"))
      .sort($"o_totalprice".desc, $"o_orderdate")
      .limit(100)
  }

  def execute_Q19(desc: Description, session: SparkSession, params: List[Any]) = {

    import session.implicits._

    val lineitem: DataFrame = desc.lineitem
    val part = desc.part

    val sm = udf { (x: String) => x.matches("SM CASE|SM BOX|SM PACK|SM PKG") }
    val md = udf { (x: String) => x.matches("MED BAG|MED BOX|MED PKG|MED PACK") }
    val lg = udf { (x: String) => x.matches("LG CASE|LG BOX|LG PACK|LG PKG") }

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    // project part and lineitem first?
    part.join(lineitem, $"l_partkey" === $"p_partkey")
      .filter(($"l_shipmode" === "AIR" || $"l_shipmode" === "AIR REG") &&
        $"l_shipinstruct" === "DELIVER IN PERSON")
      .filter(
        (($"p_brand" === "Brand#12") &&
          sm($"p_container") &&
          $"l_quantity" >= 1 && $"l_quantity" <= 11 &&
          $"p_size" >= 1 && $"p_size" <= 5) ||
          (($"p_brand" === "Brand#23") &&
            md($"p_container") &&
            $"l_quantity" >= 10 && $"l_quantity" <= 20 &&
            $"p_size" >= 1 && $"p_size" <= 10) ||
            (($"p_brand" === "Brand#34") &&
              lg($"p_container") &&
              $"l_quantity" >= 20 && $"l_quantity" <= 30 &&
              $"p_size" >= 1 && $"p_size" <= 15))
      .select(decrease($"l_extendedprice", $"l_discount").as("volume"))
      .agg(sum("volume"))
  }

  def execute_Q20(desc: Description, session: SparkSession, params: List[Any]) = {
    import session.implicits._

    var lineitem: DataFrame = null
    val nation = desc.nation
    val partsupp = desc.partsupp
    val supplier = desc.supplier
    val part = desc.part

    if (desc.samples == null) {
      println("Using whole dataframe")
      lineitem = desc.lineitem
      //      lineitem.show(20)

    } else {
      println("Using sample")
      lineitem = session.createDataFrame(desc.samples(0).asInstanceOf[RDD[Row]], desc.lineitem.schema)
      //      lineitem.show(20)
    }

    val forest = udf { (x: String) => x.startsWith("forest") }

    val flineitem = lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01")
      .groupBy($"l_partkey", $"l_suppkey")
      .agg((sum($"l_quantity") * 0.5).as("sum_quantity"))

    val fnation = nation.filter($"n_name" === "CANADA")
    val nat_supp = supplier.select($"s_suppkey", $"s_name", $"s_nationkey", $"s_address")
      .join(fnation, $"s_nationkey" === fnation("n_nationkey"))

    part.filter(forest($"p_name"))
      .select($"p_partkey").distinct
      .join(partsupp, $"p_partkey" === partsupp("ps_partkey"))
      .join(flineitem, $"ps_suppkey" === flineitem("l_suppkey") && $"ps_partkey" === flineitem("l_partkey"))
      .filter($"ps_availqty" > $"sum_quantity")
      .select($"ps_suppkey").distinct
      .join(nat_supp, $"ps_suppkey" === nat_supp("s_suppkey"))
      .select($"s_name", $"s_address")
      .sort($"s_name")

  }
}
