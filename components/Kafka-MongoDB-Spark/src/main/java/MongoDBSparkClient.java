import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;



public class MongoDBSparkClient
{
	private static Date startingDate;
	private static final int timeWindowInSeconds = 3 * 60; // three minutes
	private static final int topNLanguages = 3;
	private static final int topNNations = 3;


	public static void main(String[] args) throws InterruptedException
	{
	    SparkSession spark = SparkSession.builder()
	      .master("local")
	      .appName("MongoSparkConnectorIntro")
	      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/github_events.events")
	      .getOrCreate();

	    // Create a JavaSparkContext using the SparkSession's SparkContext object
	    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

	    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

	    Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				jsc.close();
			}
		});

	    while(true)
	    {
	    	startingDate = getSecondsAgoDate(timeWindowInSeconds);

	    	JavaRDD<Document> recentDocuments =
	    		rdd
	    		.filter(filterAfterStartingDate);

	    	printTopLanguages(recentDocuments);
	    	printTopNations(recentDocuments);
	    }
	}



	public static void printTopLanguages(JavaRDD<Document> recentDocuments)
	{
	    List<Tuple2<String, Long>> topLanguages =
			recentDocuments
	    	.groupBy(groupByLanguage)
	    	.map(languageCount)
	    	.sortBy(sortByLanguageCount, false, 1)
	    	.collect();

		System.out.println("The " + topNLanguages + " most popular languages : ");

		for(int i = 0; i < Integer.min(topLanguages.size(), topNLanguages); i++)
			System.out.println(topLanguages.get(i));

		System.out.println();
	}



	public static void printTopNations(JavaRDD<Document> recentDocuments)
	{
	    List<Tuple2<String, Long>> topNations =
			recentDocuments
	    	.groupBy(groupByNation)
	    	.map(nationCount)
	    	.sortBy(sortByNationCount, false, 1)
	    	.collect();

		System.out.println("The " + topNNations + " most active nations : ");

		for(int i = 0; i < Integer.min(topNations.size(), topNNations); i++)
			System.out.println(topNations.get(i));

		System.out.println();
	}



	private static Date getSecondsAgoDate(int seconds)
	{
		return new Date(System.currentTimeMillis() - seconds * 1000);
	}



	private static Function<Document, Boolean> filterAfterStartingDate =
		new Function<Document, Boolean>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Document doc) throws Exception
			{
				return doc.getDate("timestamp").after(startingDate);
			}
		};



	private static Function<Document, String> groupByLanguage =
		new Function<Document, String>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Document doc) throws Exception
			{
				return doc.getString("language");
			}
		};



	private static Function<Document, String> groupByNation =
			new Function<Document, String>()
			{
				private static final long serialVersionUID = 1L;

				@Override
				public String call(Document doc) throws Exception
				{
					return doc.getString("nation");
				}
			};



	private static Function<Tuple2<String, Iterable<Document>>, Tuple2<String, Long>> stringCount =
		new Function<Tuple2<String, Iterable<Document>>, Tuple2<String, Long>>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Long> call(Tuple2<String, Iterable<Document>> t) throws Exception
			{
				return new Tuple2<String, Long>(t._1, t._2.spliterator().getExactSizeIfKnown());
			}
		};



	private static Function<Tuple2<String, Iterable<Document>>, Tuple2<String, Long>>
	languageCount = stringCount;



	private static Function<Tuple2<String, Iterable<Document>>, Tuple2<String, Long>>
	nationCount = stringCount;



	private static Function<Tuple2<String, Long>, Long> sortByCount =
		new Function<Tuple2<String, Long>, Long>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Tuple2<String, Long> t) throws Exception
			{
				return t._2;
			}
		};



	private static Function<Tuple2<String, Long>, Long> sortByLanguageCount =
	sortByCount;



	private static Function<Tuple2<String, Long>, Long> sortByNationCount =
	sortByCount;
}
