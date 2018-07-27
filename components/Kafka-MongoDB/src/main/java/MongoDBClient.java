import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;



public class MongoDBClient
{
	private static final int timeWindowInSeconds = 3 * 60; // three minutes
	private static final int topLanguagesLimit = 3;
	private static final int topNationsLimit = 3;


	public static void main(String[] args) throws Exception
	{
		MongoClient client = new MongoClient("localhost", 27017);
		MongoDatabase db = client.getDatabase("github_events");
		MongoCollection<Document> eventsCollection = db.getCollection("events");

		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				client.close();
			}
		});

		while(true)
		{
			printTopLanguages(eventsCollection);
			printTopNations(eventsCollection);
		}
	}



	private static Date getSecondsAgoDate(int seconds)
	{
		return new Date(System.currentTimeMillis() - seconds * 1000);
	}



	private static void printTopLanguages(MongoCollection<Document> collection) throws InterruptedException
	{
		System.out.println("The " + topLanguagesLimit + " most popular languages : ");

		collection.aggregate
		(
			Arrays.asList
			(
				Aggregates.match(Filters.gt("timestamp", getSecondsAgoDate(timeWindowInSeconds))),
				Aggregates.group("$language", Accumulators.sum("count", 1)),
				Aggregates.sort(Sorts.descending("count")),
				Aggregates.limit(topLanguagesLimit)
			)
		).forEach(printBlock);

		System.out.println();

		TimeUnit.MILLISECONDS.sleep(500);
	}



	private static void printTopNations(MongoCollection<Document> collection) throws InterruptedException
	{
		System.out.println("The " + topNationsLimit + " most active nations : ");


		collection.aggregate
		(
			Arrays.asList
			(
				Aggregates.match(Filters.gt("timestamp", getSecondsAgoDate(timeWindowInSeconds))),
				Aggregates.group("$nation", Accumulators.sum("count", 1)),
				Aggregates.sort(Sorts.descending("count")),
				Aggregates.limit(topNationsLimit)
			)
		).forEach(printBlock);

		System.out.println();

		TimeUnit.MILLISECONDS.sleep(500);
	}



	private static Block<Document> printBlock =
		new Block<Document>()
		{
	        @Override
	        public void apply(final Document document)
	        {
	            System.out.println(document.toJson());
	        }
	    };
}
