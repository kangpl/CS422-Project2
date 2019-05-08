package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Sampler {
  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    // TODO: implement
    
    // 0) ORDERKEY, 1) PARTKEY, 2) SUPPKEY, 3) LINENUMBER, 4) QUANTITY, 5) EXTENDEDPRICE, 6) DISCOUNT, 7) TAX, 8) RETURNFLAG, 
    // 9) LINESTATUS, 10) SHIPDATE, 11) COMMITDATE, 12) RECEIPTDATE, 13) SHIPINSTRUCT, 14) SHIPMODE, 15) COMMENT
    
    //Schema:root
    // |-- l_orderkey: long (nullable = true)
    // |-- l_partkey: long (nullable = true)
    // |-- l_suppkey: long (nullable = true)
    // |-- l_linenumber: integer (nullable = true)
    // |-- l_quantity: decimal(12,2) (nullable = true)
    // |-- l_extendedprice: decimal(12,2) (nullable = true)
    // |-- l_discount: decimal(12,2) (nullable = true)
    // |-- l_tax: decimal(12,2) (nullable = true)
    // |-- l_returnflag: string (nullable = true)
    // |-- l_linestatus: string (nullable = true)
    // |-- l_shipdate: date (nullable = true)
    // |-- l_commitdate: date (nullable = true)
    // |-- l_receiptdate: date (nullable = true)
    // |-- l_shipinstruct: string (nullable = true)
    // |-- l_shipmode: string (nullable = true)
    // |-- l_comment: string (nullable = true)
    
    // Calculate K
    /*QCS
    {8, 9, 10} 					l_shipdate l_returnflag l_linestatus
    {0, 10} 							l_orderkey l_shipdate
    {0, 2} 							l_orderkey l_suppkey
    {4, 6, 10} 					l_shipdate l_discount l_quantity 
    {0, 2, 10} 					l_orderkey l_suppkey l_shipdate
    {0, 1, 2} 						l_orderkey l_suppkey l_partkey 
    {0, 8} 							l_orderkey l_returnflag
    {0, 10, 11, 12, 14} 	l_orderkey l_shipmode l_commitdate l_receiptdate l_shipdate
    {1, 4} 							l_partkey l_quantity
    {0, 4} 							l_orderkey l_quantity
    {1, 4, 13, 14} 				l_partkey l_quantity l_shipmode l_shipinstruct
    {1, 2, 10}						l_partkey l_suppkey l_shipdate
    */
    
    //
    
    
//    lineitem.groupby("col_
    null
  }
}
