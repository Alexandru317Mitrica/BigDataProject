import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import org.apache.spark.rdd._
import org.apache.spark._
import _root_.kafka.serializer.StringDecoder

import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import java.util.Properties
import org.apache.spark.sql.SaveMode
import sys.process._


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import scala.concurrent.{Await, Future}
import scala.language.postfixOps

import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row;

import org.apache.spark.sql.types._
import java.util.Properties


import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps


object ScalaStreaming {

case class Dates(date: String, 
		 protocolIP: String, 
		 source_IP: String, 
		 source_port : Double,  
		 destination_IP : String, 
		 destination_port : Double, 
		 protocolTCP: String, 
		 lengthsource: Double)

  def main(args: Array[String]) {

  	val config = new SparkConf()
        val sc = new SparkContext(config)

        val sqlContext= new org.apache.spark.sql.SQLContext(sc)

	import sqlContext.implicits._

        val ssc = new StreamingContext(sc, Seconds(5))

     //kafka set-up

               val brokers = "172.20.0.7:9092"
               val topics = "network-topic"
               val topicsSet = topics.split(",").toSet


               val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
	  
	  

  val schema = StructType(Array(
		StructField("date", StringType, true),
		StructField("protocolIP", StringType, true),
           	StructField("source_IP", StringType, true),
           	StructField("source_port", DoubleType, true),
		StructField("destination_IP", StringType, true),
		StructField("destination_port", DoubleType, true),
		StructField("protocolTCP", StringType, true),
		StructField("lengthsource", DoubleType, true) ))



val prop = new java.util.Properties()
prop.put("user", "root")
prop.put("password", "<passwd>")

val driver = "com.mysql.jdbc.Driver"
val url = "jdbc:mysql://localhost:3306/networkmonitoring"
	  

val hdfsdir ="/tmp/Zepp_date"

 val linesDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)



val uDStream = linesDStream.map(_._2).map(_.split(",")).map(p => Dates(p(0).toString,
								       p(1).toString,
								       p(2).toString,
								       p(3).toDouble,  
								       p(4).toString, 
								       p(5).toDouble,
								       p(6).toString, 
								       p(7).toDouble))


uDStream.foreachRDD{ rdd =>

	if (!rdd.isEmpty) {
		 
                  val now: Long = System.currentTimeMillis 
 		
        	  val count = rdd.count
        	  println("count received " + count)

		  rdd.saveAsTextFile(hdfsdir+"_f_"+now.toString())

        	  val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
       	       
                  val df = rdd.toDF()



//extract mysql tablespace rows through dataframes


  val d_test = sqlContext.read.format("jdbc").options(
        Map(
          "url" -> "jdbc:mysql://localhost:3306/networkmonitoring?user=root&password=<password>",
          "dbtable" -> "networkmonitor",
          "driver" -> "com.mysql.jdbc.Driver"
        )).load()

  d_test.show()

        //write to MySQL 

  df.write.mode(SaveMode.Append).jdbc(url,"networkmonitor",prop)


        }

     }
ssc.start()             
ssc.awaitTermination()  
   }
}
