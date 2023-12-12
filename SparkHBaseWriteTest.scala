package org.pdn

import it.nerdammer.spark.hbase.rddToHBaseBuilder
import org.apache.spark.{SparkConf, SparkContext}

object SparkHBaseWriteTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()

    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.hbase.host", "UDM-APP1:2181,UDM-APP2:2181,UDM-APP3:2181")
    sparkConf.setAppName("Spark HBase Writer")
    //
    val sc = new SparkContext(sparkConf)

    sc.hadoopConfiguration.set("spark.hbase.host", "UDM-APP1:2181,UDM-APP2:2181,UDM-APP3:2181")
    val rdd = sc.parallelize(1 to 100)
      .map(i => (i.toString, i + 1, "Hello" + i.toString))
    rdd.toHBaseTable("test").toColumns("col1", "col2").inColumnFamily("cf").save()
  }
}

//                        rddFromFile.map(row => )
//val rdd = sc.parallelize(1 to 100)
//.map(i => (i.toString, i + 1, "Hello"))
//rdd.toHBaseTable("bmi")
//                  .toColumns("Bank_Code", "Account_No", "cust_No", "Account_Type", "Account_Type_Desc", "Account_Status", "Account_Status_Desc", "Open_Branch", "UnitNameFA", "BACCNo", "Open_Date", "Freeze_Date", "Close_Date", "Change_Date", "Sheba_No", "cmn_acc", "filename", "hash", "No")
//                  .inColumnFamily("account")
//                  .save()
