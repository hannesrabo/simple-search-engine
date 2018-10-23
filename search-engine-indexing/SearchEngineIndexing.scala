package SearchEngineIndexing

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

// Hadoop input
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}

import java.net.URL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.SQLContext

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object SearchEngineIndexing {

	def getRanks() = {
		val spark = SparkSession
					.builder
					.master("local[*]")
					.appName("SparkSQL Web Indexing")
					.getOrCreate()
		
		import spark.implicits._

		// Splitting input data
		val conf = new Configuration
		conf.set("textinputformat.record.delimiter", "WARC/1.0")
		val dataset = spark
			.sparkContext
			.newAPIHadoopFile(
				// "../data/WAT/",
				"file:///home/hrabo/Documents/skola/data-intensive-computing/project/data/WAT/*.wat",
				classOf[TextInputFormat],
				classOf[LongWritable],
				classOf[Text],
				conf
			)

		val data = dataset
			.map(x => x._2.toString)
			.filter(_.nonEmpty)
			.map(_.split("\r\n\r\n"))
			.map(arr => (arr(0), arr(1)))
			.filter(_._2.startsWith("{"))

		val uriMetaDataPairs = data
			.map(pair => {
				val (header, metadata) = pair

				val keyValuePairs = header
					.split("\r\n")
					.filter(_ != "")
					.map(line => {
						val splitList = line.split(": ")
						((splitList(0), splitList(1)))
					})

				val targetURL = keyValuePairs
					.filter(_._1 == "WARC-Target-URI")(0)
					._2

				val protocolRegex = """^https?:\/\/.*"""

				if (!targetURL.matches(protocolRegex)) {
					("", metadata)
				} else {
					val domain = try {
						new URL(targetURL).getHost()
					} catch {
						case e: Exception => ""
					}

					(domain, metadata)
			}

		})
		.filter(_._1.nonEmpty)

		val uriLinkPairs = uriMetaDataPairs
			.map(pair => {
				val (uri, unparsedMetaData) = pair

				val linkArrayStartIndex = unparsedMetaData.indexOf("\"Links\":") match {
					case x if (x > 0) => (x + ("\"Links\":[").length)
					case _ => 0
				}

				if (linkArrayStartIndex == 0) {
					(uri, Array[String]())
				} else {
					val unparsedLinkArray = unparsedMetaData
						.substring(linkArrayStartIndex)
						.split("]")(0)

					val links = unparsedLinkArray
						.split(",")
						.filter(_.contains("url"))
						.map(jsonObject => {
							val startIndex = jsonObject.indexOf("\"url\":") + ("\"url\":").length + 1
							val stopIndex = jsonObject.length - 2

							if (startIndex < stopIndex) {
								jsonObject.substring(startIndex, stopIndex)
							} else {
								""
							}
					})
					.filter(_ != "")

					(uri, links)
				}
			})
			.filter(_._2.nonEmpty)
		
		val uriStaticLinkPairs = uriLinkPairs
			.map(pair => {
				val (uri, links) = pair

				val protocolRegex = """^https?:\/\/.*"""

				val staticLinks = links
					.filter(_.matches(protocolRegex))
					.map(_.replaceAll("""^https?:\/\/""", """http://"""))
					.map(url => try {
						new URL(url).getHost()
					} catch {
						case e: Exception => ""
					})
					.filter(!_.isEmpty)

				(uri, staticLinks)
			})
			.reduceByKey((x, y) => x ++ y)

		val uriLinkPairsWithIndex = uriStaticLinkPairs
			.zipWithIndex
			.map(x => (x._2.toLong, x._1._1, x._1._2))

		val linksList = uriLinkPairsWithIndex.flatMap(pair => {
			val (index, uri, links) = pair

			links.map(link => (index, uri, link))
		})

		val uriList = uriLinkPairsWithIndex.map(x => (x._1, x._2))

		val linksDF = linksList.toDF("indexFrom", "from", "to").filter("from != to")
		val uriDF = uriList.toDF("index", "uri")

		val edgeDF = linksDF
			.join(uriDF, linksDF("to") === uriDF("uri"))
			.withColumnRenamed("index", "indexTo")

		val edges = edgeDF
			.select("indexFrom", "indexTo")
			.rdd
			.map(arr => Edge(
				arr(0).asInstanceOf[Long],
				arr(1).asInstanceOf[Long],
				1
			))

		val linkGraph = Graph(uriList, edges)
		val rankGraph = linkGraph.pageRank(0.0001).cache
		val ranks = rankGraph.vertices.sortBy(- _._2)
		
		val ranksDF = ranks.toDF("index", "pageRank")
		val rankedUriDF = uriDF.join(ranksDF, "index")
		
		rankedUriDF.select("uri", "pageRank").withColumnRenamed("uri", "domain")
	}

	def main(args: Array[String]) {

		// connect to Cassandra and make a keyspace and table as explained in the document
		val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
		val session = cluster.connect()

		// Create sparksession
		val spark = SparkSession
					.builder
					.master("local[*]")
					.appName("SparkSQL Web Indexing")
					.config("spark.cassandra.connection.host", "127.0.0.1")
					.config("spark.cassandra.output.batch.size.bytes", "5000")
					.config("spark.cassandra.output.concurrent.writes", "10")
					// This improves cassandra performance but can cause instability
					// We should probably not use this...
					// .config("spark.cassandra.output.batch.grouping.key", "replica_set") 
					.getOrCreate()

		
		import spark.implicits._

		spark.sparkContext.setLogLevel("ERROR")

		// Reading the file from disk
		val conf = new Configuration
		conf.set("textinputformat.record.delimiter", "WARC/1.0") // Splitting multiline format
		val dataset = spark
					.sparkContext
					.newAPIHadoopFile(
						"file:///home/hrabo/Documents/skola/data-intensive-computing/project/data/WET/*.wet",
							// "../data/WET/*.wet",
						classOf[TextInputFormat], 
						classOf[LongWritable], 
						classOf[Text], 
						conf
					)

		var tempdata = dataset.map(x=>x._2.toString).toDF();

		// Filter empty stuff
		tempdata = tempdata.filter(length($"value") > 0)

		// Split header
		tempdata = tempdata.withColumn("header", 
								split($"value", "\r\n\r\n").getItem(0)
							)
							.withColumn("text", 
								split($"value", "\r\n\r\n").getItem(1)
							)
							.drop("value")

		// preparse header
		tempdata = tempdata.withColumn("header",
								split($"header", "\r\n")
							)

		// Filter on warc type (remove info)
		tempdata = tempdata.withColumn("warc-type",
							($"header").getItem(1)
						)
						.filter($"warc-type" === "WARC-Type: conversion")
						.drop("warc-type")


		// Extract uri
		tempdata = tempdata.withColumn("uri",
								($"header").getItem(2)
							)
							.withColumn("uri",
								split($"uri", ": ").getItem(1)
							)
							.drop("header")

		// Extract domain
		val regexpr = """[^((https?):\/\/)]((www|www1)\.)?([\w-\.]+)"""
		tempdata = tempdata.withColumn("domain",
							regexp_extract($"uri", regexpr, 0)
						)
						

		// Adding the pagerank
		val ranksDF = getRanks()
		val pageInfoDF = tempdata.join(ranksDF, "domain")

		val extractKeyWords = udf[Array[(String, Int)], String](input => {
			val stopWords = Set("i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves","he","him","his","himself","she","her","hers","herself","it","its","itself","they","them","their","theirs","themselves","what","which","who","whom","this","that","these","those","am","is","are","was","were","be","been","being","have","has","had","having","do","does","did","doing","a","an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against","between","into","through","during","before","after","above","below","to","from","up","down","in","out","on","off","over","under","again","further","then","once","here","there","when","where","why","how","all","any","both","each","few","more","most","other","some","such","no","nor","not","only","own","same","so","than","too","very","s","t","can","will","just","don","should","now")
			
			input
			.split("[\\s|\n]")
			.map(s => s.replaceAll("""[^\x00-\x7F]""", "")) // We only manage ascii chars
			.map(s => s.replaceAll("""^\p{Punct}*$""", "")) // No words with only punctuation
			.map(s => s.replaceAll("""\p{Punct}$""", ""))   // No trailing punctuations
			.filter(_ != "")                                // No empty words
			.map(_.toLowerCase)
			.filter(!stopWords.contains(_))                 // Remove stop-words
			.groupBy(identity).mapValues(_.size).toArray
			.sortBy(- _._2)
		})

		// Extract keywords from text.
		val keywordDF = pageInfoDF.withColumn(
			"keywords",
			extractKeyWords(col("text"))
		)

		// Some Stats
		// keywords distinct = 2017741 (.distinct.count)
		// keywords total = 15 000 000
		// total_nr_records = 39653

		// create one list with all keywords
		val keywordsExpandedDF = keywordDF
								.withColumn("keywords", explode(($"keywords")))
								.withColumn("keyword", $"keywords._1")
								.withColumn("keyword_weight", $"keywords._2")
								.drop("keywords")

		// Creating the reversed index
		var reverseIndex = keywordsExpandedDF
							.groupBy($"keyword")	// This is very heavy...
							.agg(
								collect_list($"uri").alias("links"), 
								collect_list($"keyword_weight").alias("occurences"),
								collect_list($"pageRank").alias("page_ranks")
								)
							// Apparently sorting on partition key can drastically improve performance. This is probably not the way to go though	
							// .sortBy($"keyword")						  			
							.select("keyword", "links", "occurences", "page_ranks") // Make sure we are not getting any stray columns

		// Set up database
		session.execute("CREATE KEYSPACE IF NOT EXISTS search WITH REPLICATION =" +
						"{'class': 'SimpleStrategy', 'replication_factor': 1};")
		session.execute("CREATE TABLE IF NOT EXISTS search.keywords (keyword text PRIMARY KEY, links list<text>, occurences list<int>, page_ranks list<double>);")

		// Pushing the results to a cassandra database
		reverseIndex
			.write
			.format("org.apache.spark.sql.cassandra")
			.cassandraFormat("keywords", "search")
		    // .mode(SaveMode.Replace)
			.save()

		session.close()
	}
}
