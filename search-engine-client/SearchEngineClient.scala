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

import scala.Console

object searchEngineClient {
  def main(args: Array[String]) {

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
      println("\n=========================================")
      print("Input search term: ")
      input = scala.io.StdIn.readLine()

      if (input != "") {
        val resultRDD = dataFrame
          .filter($"keyword" === input)
          .rdd
          .flatMap(row => {
            val links = row.getAs[Seq[String]](1)
            val occurences = row.getAs[Seq[Int]](2)
            val pageRanks = row.getAs[Seq[Double]](3)

            (links, occurences, pageRanks).zipped.toList
          })
          .map(x => (x._1, x._2 * x._3))
          .sortBy(-_._2)
          .map(x => {
            val roundedScore = "%2.4f".format(x._2)
            (x._1, roundedScore)
          })

        val results = resultRDD.take(20)

        println("\n" + Console.GREEN + "Results: " + Console.RESET + "\n")
        results.foreach(result =>
          println(
            "[" + Console.GREEN + result._2 + Console.RESET + "] " + result._1))

        println("Showing 20 of " + resultRDD.count() + " results")

      }
    } while (input != "");

    spark.stop
  }
}
