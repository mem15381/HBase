//package org.pdn
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
//import org.apache.hadoop.mapreduce.Job
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.KeyValue
//import org.apache.hadoop.hbase.client.Put
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.catalyst.util.StringUtils
//import org.apache.spark.sql.{Row, DataFrame, SparkSession}
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.util.Bytes
//
//object writeToHbaseFromCSV {
//
//  System.setProperty("hadoop.home.dir", "/home/ebrahimpour/hadoop");
//
//  def main(args: Array[String]) {
//
//    val spark: SparkSession = SparkSession.builder().master("local").appName("").getOrCreate()
//
//    val sc: SparkContext = spark.sparkContext
//
//    val df=  spark.read.format("csv").option("header", "true")
//      .load("LOCATION ON HDFS")
//    df.show()
//
//    val prepareHBaseToLoad: RDD[(ImmutableBytesWritable, Put)] =
//      hbase_df.rdd.map(row =>  rowToPut(row:Row)   )
//
//    // Perform the necessary configurations in preparation for saving to HBase
//    val tableName = "Dynamic_Insertion_Hbase"
//
//    //create configuration object
//    val conf: Configuration = HBaseConfiguration.create()
//
//    //set zookeeper information
//    conf.set("hbase.zookeeper.quorum", "SERVERINFORMATION");
//    conf.set("hbase.zookeeper.property.clientPort", "PORT NUMBER");
//    //setup job object
//    val job: Job = Job.getInstance(conf)
//    //define outputformat class
//    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
//    //add table name to configuration
//    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName)
//
//    // Save the data to HBase
//    try {
//      prepareHBaseToLoad.saveAsNewAPIHadoopDataset(job.getConfiguration())
//    } catch {
//      //handle the null string excpetion while inserting to Hbase throws
//      case e: Exception => {
//        if (e.getMessage().equals("Can not create a Path from a null string")) {
//          println(" saveAsNewAPIHadoopDataset - Exception caused due to a bug in spark 2.2 - Data is saved in HBASE but still excepton is thrown - java.lang.IllegalArgumentException: Can not create a Path from a null string at org.apache.hadoop.fs.Path.checkPathArg ")
//        } else {
//          throw (e)
//        }
//      }
//    }
//  }
//
//  def rowToPut(row:Row): (ImmutableBytesWritable, Put) = {
//    //  rowToPut(row)
//    val columnList = row.length
//
//    //convert the rowKey into String
//    val rowKey: String = System.currentTimeMillis().toString
//    val arrayList = row.schema.fieldNames
//    var put = new Put(Bytes.toBytes(rowKey))
//    println(rowKey)
//    for (field <- 0 until arrayList.size) {
//      // Add the score data columns to the Put object
//      put.addColumn(Bytes.toBytes("ColumnFamily1"), Bytes.toBytes(arrayList(field)),  Bytes.toBytes(row.getAs[String](arrayList(field))))
//    }
//    // Returns the assembled Put object
//    return (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put)
//
//  }
//}