package searchEngineClient

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext

import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf

import scala.collection.mutable.WrappedArray

object searchEngineClient {
  def main(args: Array[String]) {
    // val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    // val session = cluster.connect()

    // // Create sparksession
    // val spark = SparkSession.builder
    //   .master("local[*]")
    //   .appName("SparkSQL")
    //   .config("spark.cassandra.connection.host", "127.0.0.1")
    //   .config("spark.cassandra.output.batch.size.bytes", "5000")
    //   .config("spark.cassandra.output.concurrent.writes", "10")
    //   .getOrCreate()

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkSQL")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .config("spark.cassandra.output.batch.size.bytes", "5000")
      .config("spark.cassandra.output.concurrent.writes", "10")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val dataFrame = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "search", "table" -> "keywords"))
      .load()

    var input = ""

    do {
      println("Input search term: ")
      input = scala.io.StdIn.readLine()

      if (input != "") {
        val resultRDD = dataFrame
          .filter($"keyword" === input)
          .rdd
          .flatMap(row => {
            // val keyword = row.get(0).asInstanceOf[String]
            val links = row.get(0).asInstanceOf[WrappedArray[String]].toSeq
            //   val occurences = row.get(2).asInstanceOf[WrappedArray[Int]]
            //   val pageRanks = row.get(3).asInstanceOf[WrappedArray[Double]]

            //   // val temp = ((links zip occurences) zip pageRanks) //.map(x => (keyword, x._1, x._2, x._3))

            //   // temp
            // Seq(links)
            links
          })

        resultRDD.take(2).foreach(println)
      }
    } while (input != "");
  }
}
