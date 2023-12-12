package org.pdn

import it.nerdammer.spark.hbase._
import org.apache.spark.{SparkConf, SparkContext}

object SparkHBaseRead {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.hbase.host", "UDM-APP1,UDM-APP2,UDM-APP3")
    //
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("spark.hbase.host", "UDM-APP1,UDM-APP2,UDM-APP3")

    val hBaseRDD = sc.hbaseTable[(String, Int, String)]("bank")
      .select("bank_code", "account_code")
      .inColumnFamily("account")
      .withStartRow("00000")
      .withStopRow("00500")
  }
}
