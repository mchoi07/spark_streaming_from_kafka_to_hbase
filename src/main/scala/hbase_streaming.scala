/**
  * Hbase version local : 1.2.9
  * Hbase version Hdp: 1.1.2
  * */

import scala.util.parsing.json.JSON

// spark libraries
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


// kafka libraries
import org.apache.kafka.common.serialization.StringDeserializer

// hbase libraries
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

case class Twitter(created_at: String, id: String, text: String)

object Twitter extends Serializable{

  final val tableName = "" // please set the table name 
  final val cfDataBytes = Bytes.toBytes("data")
  final val colText = Bytes.toBytes("text")

  def parseTwitter(str: String): Twitter = {

    val (v1, v2, v3) = JSON.parseFull(str).collect { case map: Map[String, Any] => (map("created_at"), map("id"), map("text")) }.get
    Twitter(v1.toString, v2.toString, v3.toString)
  }

  def put_converter(twitter: Twitter) : (ImmutableBytesWritable, Put) =  {
    val create_date = twitter.created_at
    // create a composite row key: sensorid_date time
    val rowkey = twitter.id + "_" + create_date
    val put = new Put(Bytes.toBytes(rowkey))

    // add to column family data, column  data values to put object
    put.add(cfDataBytes, colText, Bytes.toBytes(twitter.text))
    return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
  }

}

object hbase_streaming {

  def main(args: Array[String]): Unit = {

    // ************ Spark Configuration ************ //

    val conf = new SparkConf()
      .setAppName("HBaseStream")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    // val ssc = new StreamingContext(conf, Seconds(1))

    // ************ Kafka Configuration ************ //

    val topics = List("twitter").toSet // Your topic name

    val kafkaParams = Map(
      "bootstrap.servers" -> "", // Your server
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "consumer-group", // Your consumer group
      "auto.offset.reset" -> "earliest")

    // ************ Hbase Configuration ************ //

    val hbconf = HBaseConfiguration.create()
    hbconf.set(TableOutputFormat.OUTPUT_TABLE, Twitter.tableName)
    hbconf.set("hbase.zookeeper.quorum", "") // Your input
    hbconf.set("zookeeper.znode.parent", "") // Your input
    hbconf.set("hbase.master", "") // Your Master Address and port
    //hbconf.set("hbase.rootdir", "hdfs://") // Hdfs directory 

    val jobConfig: JobConf = new JobConf(hbconf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "output")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, Twitter.tableName)

    // ************ Configuration Done here ************ //

    // Getting the Data from Kafka into Dstream Object
    // Getting streaming data from Kafka and send it to the Spark and create Dstream RDD

    val kafka_stream_Dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    // Transformation Sample - 1 : converting all characters to lower cases
    // Transformation Sample - 2 : Append the Rdd to the Class "Twitter"

    val lower_Dstream = kafka_stream_Dstream.map(record => record.value().toString.toLowerCase).map(Twitter.parseTwitter)

    lower_Dstream.print()

    lower_Dstream.foreachRDD(rddRaw => {

      // convert twitter data to put object and write to Hbase into CF data
      rddRaw.map(Twitter.put_converter).saveAsHadoopDataset(jobConfig)
    }
  )

    ssc.start()
    ssc.awaitTermination()
  }
}
