package org.pdn

import it.nerdammer.spark.hbase.rddToHBaseBuilder
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object SparkHBaseWriteFiles {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkHBaseWrite").setMaster("local[*]")
    sparkConf.set("spark.hbase.host", "UDM-APP1:2181,UDM-APP2:2181,UDM-APP3:2181")
    //

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    sc.hadoopConfiguration.set("spark.hbase.host", "UDM-APP1:2181,UDM-APP2:2181,UDM-APP3:2181")

    //.map(f => (f.split(','))).zipWithIndex().map(x => (x._2, x._1)).
    val sh_regex = "^(?:IR)(?=.{24}$)[0-9]*$"
    val bankCode_regex = "^[\"]+|\\s?[\"]+$"
    val pathFiles = args(0)
    val rdd = sc.textFile(pathFiles)
    rdd.foreach(r => {
      println(r)

      //"/mnt/BG1_Share/*/*/*A[CCOUNT-ccount]*True.csv"

      val rddFromFile = r.map(f => f.stripLineEnd.split(",")).map(f => (clns(f(0) + f(1)), clns(f(0)), clns(f(1)), clns(f(2)), clns(f(3)), clns(f(4)), clns(f(5)), clns(f(6)), clns(f(7)), clns(f(8)), clns(f(9)), clns(f(10)), clns(f(11)), isMatch(clns(f(12)), sh_regex), clns(f(13)), clns(f(14)), clns(f(15)), clns(f(16))))
      //rddFromFile.map(f => f.split(',')).map(f => (f(0), tryToInt(f(1)), tryToInt(f(2)), tryToInt(f(3)), f(4), tryToInt(f(5)), f(6), f(7), f(8), f(9), tryToInt(f(10)), tryToInt(f(11)), tryToInt(f(12)), tryToInt(f(13)), tryToInt(f(14)), tryToInt(f(15)), tryToInt(f(16)), f(17), f(18), tryToInt(f(19))))
      rddFromFile.toHBaseTable("banks").toColumns("Bank_Code", "Account_No", "cust_No", "Account_Type", "Account_Type_Desc", "Account_Status", "Account_Status_Desc", "Open_Branch", "Open_Date", "Freeze_Date", "Close_Date", "Change_Date", "Sheba_No", "id", "filename", "hash", "no")
        .inColumnFamily("account")
        .save()
    })
  }

  def tryToInt(s: String) = Try(s.toInt).toOption.getOrElse(-1)
  def isMatch(e: String, r: String) : String = if (e.matches(r)) e else ""
  //def clns(s: String) : String = s.replaceAll("[\\-\\+\\.\\^:]","").stripLineEnd
  //def clns(s: String) : String = s.replaceAll("[^A-Za-z0-9]", "")
  def clns(s: String) : String = s.replaceAll("[\\W]|_", "");
//  def compareKey(sc: SparkContext, s : String) : HBaseReaderBuilder[(String, Int, String)] = {
//     sc.hbaseTable[(String, Int, String)]("bank")
//      .select("bank_code", "account_code")
//      .inColumnFamily("account")
//      .withStartRow(s)
//      .withStopRow(s)
//  }
}