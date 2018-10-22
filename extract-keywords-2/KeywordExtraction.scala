package keywordExtraction

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

// Hadoop input
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
// import com.datastax.spark.connector.streaming._

import java.net.URL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ 

object keywordExtraction {
  def main(args: Array[String]) {

    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    // Create sparksession
    val spark = SparkSession
                .builder
                .master("local[*]")
                .appName("SparkSQL")
                .config("spark.cassandra.connection.host", "127.0.0.1")
				.config("spark.cassandra.output.batch.size.bytes", "5000")
				.config("spark.cassandra.output.concurrent.writes", "10")
				// This improves cassandra performance but can cause instability
				// We should probably not use this...
				// .config("spark.cassandra.output.batch.grouping.key", "replica_set") 
                .getOrCreate()

	import spark.implicits._

    // Reading the file from disk
    val conf = new Configuration
    conf.set("textinputformat.record.delimiter", "WARC/1.0") // Splitting multiline format
	val dataset = spark
                .sparkContext
                .newAPIHadoopFile(
                    "file:///home/hrabo/Documents/skola/data-intensive-computing/project/data/WET/*.wet",
//                     "./data/WET/CC-MAIN-20180918130631-20180918150631-00000.warc.wet",
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
					

	val pageInfoDF = tempdata

	// We can join on domain here from pagerank!
	///////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////


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

	val keywordDF = pageInfoDF.withColumn(
		"keywords",
		extractKeyWords(col("text"))
	)
	
	// create one list with all keywords
	// keywords distinct = 2017741 (.distinct.count)
	// keywords total = 15 000 000
	// total_nr_records = 39653
	val keywordsExpandedDF = keywordDF
                            .withColumn("keywords", explode(($"keywords")))
                            .withColumn("keyword", $"keywords._1")
                            .withColumn("keyword_weight", $"keywords._2")
							.drop("keywords")

	var reverseIndex = keywordsExpandedDF
                        .groupBy($"keyword")
                        .agg(
                            collect_list($"uri").alias("links"), 
                            collect_list($"keyword_weight").alias("occurences")
                            )
						// .sortBy($"keyword")						  // Apparently sorting on partition key can drastically improve performance. This is probably not the way to go
						.select("keyword", "links", "occurences") // Make sure we are not getting any tray columns

                        // We can probably collect as struct collect_list(struct($"uri", $"keyword_weight")).as("set")
						// This would allow easier sorting here which might not be any idea anyway
                        // This might however lead to problems: https://stackoverflow.com/questions/31864744/spark-dataframes-groupby-into-list
                        
	// Showing here takes 1 minute
	// reducedKeywords.show() 

	session.execute("CREATE KEYSPACE IF NOT EXISTS search WITH REPLICATION =" +
					"{'class': 'SimpleStrategy', 'replication_factor': 1};")
	session.execute("CREATE TABLE IF NOT EXISTS search.keywords (keyword text PRIMARY KEY, links list<text>, occurences list<int>);")
	// session.close()


	reverseIndex
		.write
		.format("org.apache.spark.sql.cassandra")
		.cassandraFormat("keywords", "search")
	//     .mode(SaveMode.Append)
		.save()

    // store the result in Cassandra
    // stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    session.close()
  }
}
