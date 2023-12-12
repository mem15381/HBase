package org.pdn

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object SparkHBaseWriteFileNames {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkHBaseWrite").setMaster("local[*]")
    sparkConf.set("spark.hbase.host", "UDM-APP1:2181,UDM-APP2:2181,UDM-APP3:2181")
    //

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    sc.hadoopConfiguration.set("spark.hbase.host", "UDM-APP1:2181,UDM-APP2:2181,UDM-APP3:2181")

    //.map(f => (f.split(','))).zipWithIndex().map(x => (x._2, x._1)).
    val sheba_regex = "^(?:IR)(?=.{24}$)[0-9]*$"
    val bankCode_regex = "^[\"]+|\\s?[\"]+$"
    val pathFiles = args(0)
    val rdd = sc.textFile(pathFiles)
    rdd.foreach(r => {
      println(r)

      //"/mnt/BG1_Share/*/*/*A[CCOUNT-ccount]*True.csv"
    })
  }

  def tryToInt(s: String) = Try(s.toInt).toOption.getOrElse(-1)
  def isMatch(e: String, r: String) : String = if (e.matches(r)) e else ""
  //def clns(s: String) : String = s.replaceAll("[\\-\\+\\.\\^:]","").stripLineEnd
  //def clns(s: String) : String = s.replaceAll("[^A-Za-z0-9]", "")
  def clns(s: String) : String = s.replaceAll("[\\W]|_", "");


}