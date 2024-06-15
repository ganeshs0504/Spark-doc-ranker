package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentsToDocumentRankingsFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentRankingsReduceGroups;
import uk.ac.gla.dcs.bigdata.studentfunctions.FilterNotNullNewsFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.TupleToDocumentRankingMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryGroupMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.ProcessNewsMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.TermCountMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.TermCountReducer;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedNews;
import uk.ac.gla.dcs.bigdata.studentstructures.TermCounts;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[4]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		// if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; // Full Dataset
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		
		// Filtering out any news articles which have null titles or contents
		Dataset<NewsArticle> notNullNews = news.flatMap(new FilterNotNullNewsFlatMap(), Encoders.bean(NewsArticle.class));
		
		
		// Initialising the number of documents and the total length of all documents as accummulators
		LongAccumulator docCount = spark.sparkContext().longAccumulator();
		LongAccumulator docLengthSum = spark.sparkContext().longAccumulator();
		
		// Map function to process the news articles by given text processor for the first 5 paragraphs along with the calculation of the document count and the total length of all documents
		Dataset<ProcessedNews> processedNews = notNullNews.map(new ProcessNewsMap(docCount,docLengthSum), Encoders.bean(ProcessedNews.class));
		
		// Map and reduce functions to calculate the term counts for the entire corpus 
		Dataset<TermCounts> termCounts = processedNews.map(new TermCountMap(),Encoders.bean(TermCounts.class));
		HashMap<String,Integer> termCountsCorpus = termCounts.reduce(new TermCountReducer()).getTermCount();
		
		// Calculating the number of documents in the corpus and the average document length using the defined accummulators
		long docsInCorpus = docCount.value();
		double averageDocumentLength = docLengthSum.value() / docsInCorpus;

		// Broadcasting the term counts of the entire corpus for DPH score calculation
		Broadcast<HashMap<String,Integer>> broadcastTermCounts = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termCountsCorpus);

		// Collecting the queries as a list and broadcasting it for the calculation of the document rankings
		List<Query> queryObjAsList = queries.collectAsList();
		Broadcast<List<Query>> broadcastQueryObjAsList = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryObjAsList);

		// Return document rankings for queries, but with a separate document ranking for each document for each query
		Dataset<DocumentRanking> singleDPHDocumentRankings = processedNews.flatMap(new DocumentsToDocumentRankingsFlatMap(broadcastQueryObjAsList, docsInCorpus, broadcastTermCounts, averageDocumentLength), Encoders.bean(DocumentRanking.class));

		// Grouping the document rankings by query
		KeyValueGroupedDataset<Query,DocumentRanking> groupedSingleRankings = singleDPHDocumentRankings.groupByKey(new QueryGroupMap(), Encoders.bean(Query.class));
	
		// Reducing the grouped document rankings to get the final document rankings for each query
		Dataset<Tuple2<Query,DocumentRanking>> rankingsTuple = groupedSingleRankings.reduceGroups(new DocumentRankingsReduceGroups());
		
		// Map function to convert the tuple of query and document ranking to a list of document rankings for the final output of the pipeline
		Dataset<DocumentRanking> rankings = rankingsTuple.map(new TupleToDocumentRankingMap(),Encoders.bean(DocumentRanking.class));
		
		// Collecting the document ranking as a list for the return statement of the function
		List<DocumentRanking> rankingsAsList = rankings.collectAsList();
		
		return rankingsAsList;
	}
	
	
}
