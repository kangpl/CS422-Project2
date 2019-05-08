package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Sampler {
  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    // TODO: implement
    
    // 0) ORDERKEY, 1) PARTKEY, 2) SUPPKEY, 3) LINENUMBER, 4) QUANTITY, 5) EXTENDEDPRICE, 6) DISCOUNT, 7) TAX, 8) RETURNFLAG, 
    // 9) LINESTATUS, 10) SHIPDATE, 11) COMMITDATE, 12) RECEIPTDATE, 13) SHIPINSTRUCT, 14) SHIPMODE, 15) COMMENT
    
//     TABLE LINEITEM [6000000:S] (
//     ORDERKEY      LONG   DISTINCT=1500000:S                               ,
//     PARTKEY       LONG   DISTINCT=200000:S                                , 
//     SUPPKEY       LONG   DISTINCT=10000:S                                 ,
//     LINENUMBER    LONG                                                    ,
//     QUANTITY      DOUBLE DISTINCT=50       MIN=1.00 MAX=50.0              ,
//     EXTENDEDPRICE DOUBLE                                                  ,
//     DISCOUNT      DOUBLE DISTINCT=11	MIN=0.00 MAX=0.10              ,
//     TAX           DOUBLE                                                  , 
//     RETURNFLAG    STRING DISTINCT=3                                       ,
//     LINESTATUS    STRING DISTINCT=2                                       ,
//     SHIPDATE      DATE   DISTINCT=2518      MIN=1992-01-01 MAX=1998-12-01 ,
//     COMMITDATE    DATE   DISTINCT=2578      MIN=1992-01-01 MAX=1998-10-31 ,
//     RECEIPTDATE   DATE   DISTINCT=2548      MIN=1992-01-01 MAX=1998-09-01 ,
//     SHIPINSTRUCT  STRING DISTINCT=4                                       ,
//     SHIPMODE      STRING DISTINCT=7                                       ,
//     COMMENT       STRING
//    );
    
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
    null
  }
}
